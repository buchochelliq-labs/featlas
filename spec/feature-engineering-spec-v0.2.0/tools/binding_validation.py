"""Local structural-reference checks for deployment portability.

No adapter is loaded and no vendor configuration is interpreted by this module.
These checks do not establish that an adapter exists or can execute a plan.
"""
import json


def check_binding_semantics(document, deployment, indexes, error):
    path = f"/deployments/{deployment['id']}"
    adapters = {}
    for adapter in deployment["adapters"]:
        if adapter["id"] in adapters:
            error("DUPLICATE_ADAPTER", path + "/adapters", adapter["id"])
        adapters[adapter["id"]] = adapter

    def role_check(adapter_id, role, at):
        adapter = adapters.get(adapter_id)
        if adapter is None:
            error("UNKNOWN_ADAPTER", at, adapter_id)
        elif role not in adapter["roles"]:
            error("ADAPTER_ROLE_MISMATCH", at, f"{adapter_id} does not declare role {role}")

    def physical_columns_unique(names, at):
        if len(set(names)) != len(names):
            error("PHYSICAL_COLUMN_COLLISION", at, "Logical values, states, keys and metadata must map to distinct physical fields")

    for role, adapter_id in deployment["execution"].items():
        if adapter_id is not None:
            role_check(adapter_id, role, path + "/execution/" + role)
    if deployment["schedule"]["mode"] != "manual" and deployment["execution"]["orchestrator"] is None:
        error("ORCHESTRATOR_REQUIRED", path + "/execution/orchestrator", "A scheduled deployment requires an explicit orchestrator adapter")

    for binding in deployment["source_bindings"]:
        at = path + "/source_bindings/" + binding["source"]
        role_check(binding["adapter"], "catalog", at + "/adapter")
        source = indexes["sources"].get(binding["source"])
        if source is None:
            continue  # The core validator reports the unknown source.
        if source["version"] != binding["version"]:
            error("VERSION_MISMATCH", at, binding["source"])
        if set(binding["column_mapping"]) != {column["name"] for column in source["columns"]}:
            error("SOURCE_COLUMN_MAPPING_INCOMPLETE", at, "Map exactly every declared logical source column")
        physical_columns_unique(list(binding["column_mapping"].values()), at)

    targets = set()
    physical_targets = set()
    for target in deployment["output_targets"]:
        at = path + "/output_targets/" + target["feature_set"]
        role_check(target["adapter"], target["role"], at + "/adapter")
        key = (target["feature_set"], target["version"], target["role"])
        if key in targets:
            error("DUPLICATE_OUTPUT_TARGET", at, "Only one output target per feature-set version and storage role is allowed")
        targets.add(key)
        location = target["location"]
        if location["kind"] == "fixture":
            error("OUTPUT_LOCATION_READ_ONLY", at + "/location", "Fixture locations are read-only sources, not output destinations")
        address = {key: value for key, value in location.items() if key != "format"}
        resource = (target["adapter"], json.dumps(address, sort_keys=True, separators=(",", ":")))
        if resource in physical_targets:
            error("DUPLICATE_OUTPUT_RESOURCE", at, "Repeated physical output resources require an explicit sharing contract, outside this profile")
        physical_targets.add(resource)
        feature_set = indexes["feature_sets"].get(target["feature_set"])
        if feature_set is None:
            continue
        entity = indexes["entities"].get(feature_set["entity"])
        if entity and set(target["entity_key_mapping"]) != {key["name"] for key in entity["keys"]}:
            error("OUTPUT_ENTITY_MAPPING_INCOMPLETE", at, "Map exactly every logical entity key")
        members = {member["id"]: member["version"] for member in feature_set["features"]}
        mapped = {}
        physical = list(target["entity_key_mapping"].values()) + list(target["metadata_columns"].values())
        for mapping in target["feature_mapping"]:
            if mapping["feature"] in mapped:
                error("DUPLICATE_OUTPUT_FEATURE", at, mapping["feature"])
            mapped[mapping["feature"]] = mapping["version"]
            if mapping["feature"] in members and members[mapping["feature"]] != mapping["version"]:
                error("VERSION_MISMATCH", at, mapping["feature"])
            physical.extend([mapping["column"], mapping["state_column"]])
        if set(mapped) != set(members):
            error("OUTPUT_FEATURE_MAPPING_INCOMPLETE", at, "Map exactly every feature-set member and its state")
        physical_columns_unique(physical, at)

    required_role = "offline_store" if deployment["publication"]["surface"] == "offline_snapshot" else "online_store"
    selected_sets = {(target["feature_set"], target["version"]) for target in deployment["output_targets"]}
    for feature_set, version in selected_sets:
        if (feature_set, version, required_role) not in targets:
            error("PUBLICATION_SURFACE_UNBOUND", path + "/publication", f"{feature_set}@{version} has no {required_role} target")

    consumers = set()
    for binding in deployment["consumer_bindings"]:
        at = path + "/consumer_bindings/" + binding["consumer"]
        if binding["consumer"] in consumers:
            error("DUPLICATE_CONSUMER_BINDING", at, binding["consumer"])
        consumers.add(binding["consumer"])
        role_check(binding["adapter"], binding["role"], at + "/adapter")
        if binding["role"] != required_role:
            error("CONSUMER_SURFACE_MISMATCH", at, "A consumer must use this deployment's guaranteed publication surface; use separate deployments for other surfaces")
        consumer = indexes["consumers"].get(binding["consumer"])
        if consumer is None:
            error("UNKNOWN_CONSUMER", at, binding["consumer"])
            continue
        if consumer["version"] != binding["version"]:
            error("VERSION_MISMATCH", at, binding["consumer"])
        reference = consumer["feature_set"]
        if not any(target["feature_set"] == reference["feature_set"] and target["version"] == reference["version"]
                   and target["adapter"] == binding["adapter"] and target["role"] == binding["role"]
                   for target in deployment["output_targets"]):
            error("CONSUMER_TARGET_MISSING", at, "Consumer requires a matching feature-set output target on its selected adapter and role")

    requirements = set()
    for requirement in deployment["runtime"]["required_capabilities"]:
        at = path + "/runtime/required_capabilities"
        role_check(requirement["adapter"], requirement["role"], at)
        key = (requirement["adapter"], requirement["role"], requirement["capability"])
        if key in requirements:
            error("DUPLICATE_CAPABILITY_REQUIREMENT", at, str(key))
        requirements.add(key)

    native = deployment["portability"]["native_bindings"]
    if native and deployment["portability"]["mode"] == "portable":
        error("NATIVE_MODE_REQUIRED", path + "/portability", "Native bindings require mode allow_native")
    native_keys = set()
    for binding in native:
        at = path + "/portability/native_bindings"
        role_check(binding["adapter"], binding["role"], at)
        reference = binding["feature"]
        feature = indexes["features"].get(reference["id"])
        if feature is None:
            error("UNKNOWN_FEATURE", at, reference["id"])
        elif feature["version"] != reference["version"]:
            error("VERSION_MISMATCH", at, reference["id"])
        key = (reference["id"], reference["version"], binding["adapter"], binding["role"])
        if key in native_keys:
            error("DUPLICATE_NATIVE_BINDING", at, str(key))
        native_keys.add(key)
        suite = indexes["acceptance_suites"].get(binding["conformance_suite"])
        if suite is None:
            error("UNKNOWN_SUITE", at, binding["conformance_suite"])
        elif not any(case["feature"] == reference for case in suite["cases"]):
            error("NATIVE_SUITE_FEATURE_MISMATCH", at, "Native conformance suite must contain cases for the pinned feature")
        for capability in binding["required_capabilities"]:
            if (binding["adapter"], binding["role"], capability) not in requirements:
                error("NATIVE_CAPABILITY_UNDECLARED", at, capability)
