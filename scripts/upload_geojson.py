#!/usr/bin/env python3
import glob
import json
import os
import sys

from omero.gateway import BlitzGateway


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
    dataset_id = int(get_env("OMERO_DATASET_ID", required=True))
    csv_glob = get_env("CSV_GLOB", required=True)

    conn = BlitzGateway(username, password, host=host, port=port)
    if not conn.connect():
        die("Failed to connect to OMERO")

    try:
        if group:
            conn.setGroupForSession(group)

        dataset = conn.getObject("Dataset", dataset_id)
        if dataset is None:
            die(f"Dataset not found: {dataset_id}")

        csv_files = sorted(glob.glob(csv_glob))
        if not csv_files:
            die(f"No CSV files found for pattern: {csv_glob}")

        attached = []
        for path in csv_files:
            mimetype = "application/gzip" if path.endswith(".gz") else "text/csv"
            ann = conn.createFileAnnfromLocalFile(
                path,
                mimetype=mimetype,
                ns="omero.file.attachment"
            )
            dataset.linkAnnotation(ann)
            attached.append({
                "file": path,
                "file_annotation_id": ann.getId(),
            })

        print(json.dumps({"dataset_id": dataset_id, "attached": attached}, indent=2))

    finally:
        conn.close()


if __name__ == "__main__":
    main()