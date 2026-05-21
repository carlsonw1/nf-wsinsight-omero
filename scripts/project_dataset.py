#!/usr/bin/env python3
import json
import os
import sys

from omero.gateway import BlitzGateway
from omero.model import ProjectI, DatasetI, ProjectDatasetLinkI
from omero.rtypes import rstring


def die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    sys.exit(code)


def get_env(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.environ.get(name, default)
    if required and not val:
        die(f"Missing required environment variable: {name}")
    return val


def main() -> None:
    host = get_env("OMERO_HOST", required=True)
    port = int(get_env("OMERO_PORT", "4064"))
    username = get_env("OMERO_USERNAME", required=True)
    password = get_env("OMERO_PASSWORD", required=True)
    group = get_env("OMERO_GROUP", "")
    project_name = get_env("OMERO_PROJECT_NAME", required=True)
    dataset_name = get_env("OMERO_DATASET_NAME", required=True)
    out_json = get_env("OMERO_TARGET_JSON", "omero_target.json")

    conn = BlitzGateway(username, password, host=host, port=port)
    connected = conn.connect()
    if not connected:
        die("Failed to connect to OMERO")

    try:
        if group:
            conn.setGroupForSession(group)

        update_service = conn.getUpdateService()
        query_service = conn.getQueryService()

        # Find project by exact name in current group
        project = None
        for p in conn.getObjects("Project", attributes={"name": project_name}):
            if p.getName() == project_name:
                project = p
                break

        if project is None:
            new_project = ProjectI()
            new_project.name = rstring(project_name)
            new_project = update_service.saveAndReturnObject(new_project)
            project_id = new_project.id.val
        else:
            project_id = project.getId()

        # Find dataset with exact name linked to this project
        dataset_id = None
        project_obj = conn.getObject("Project", project_id)
        for ds in project_obj.listChildren():
            if ds.getName() == dataset_name:
                dataset_id = ds.getId()
                break

        if dataset_id is None:
            new_dataset = DatasetI()
            new_dataset.name = rstring(dataset_name)
            new_dataset = update_service.saveAndReturnObject(new_dataset)
            dataset_id = new_dataset.id.val

            link = ProjectDatasetLinkI()
            link.parent = ProjectI(project_id, False)
            link.child = DatasetI(dataset_id, False)
            update_service.saveObject(link)

        payload = {
            "project_name": project_name,
            "project_id": project_id,
            "dataset_name": dataset_name,
            "dataset_id": dataset_id,
            "group": group,
        }

        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        print(json.dumps(payload, indent=2))

    finally:
        conn.close()


if __name__ == "__main__":
    main()