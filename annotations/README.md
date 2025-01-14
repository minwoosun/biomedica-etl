# Annotations
This directory contains expert-derived annotation metadata files for PMC-OA. These files are used during the serialization process.



* ```expert_derived_taxonomy.json: dict[str, dict[str, list[str]]]``` Corresponds to the original PMC-OA taxonomy derived by three experts by looking at random samples from the 24M images and biomedical taxonomies.

* ```taxonomy_collapsed_and_filtered.json: dict[str, list[str]]``` Corresponds to the collapsed taxonomy (for ease of annotations), containing only the classes found in the clusters.

* ```cluster_labels.json: dict[str, dict[str, Union[str, list]]]``` Contains a dictionary with propagated expert annotations for each of the 2000 clusters. The key corresponds to one of the cluster labels ("0"-"1999"), and the values correspond to a dictionary with keys "panel_type", "panel_subtype", "global_class", and "local_class".
