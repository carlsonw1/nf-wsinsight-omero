#!/usr/bin/env python3
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
    import_report = get_env("IMPORT_REPORT_JSON", required=True)
    csv_path = get_env("CSV_PATH", required=True)
    sample_id = get_env("SAMPLE_ID", required=True)

    with open(import_report, "r", encoding="utf-8") as f:
        report = json.load(f)

    image_ids = report.get("image_ids", [])
    if not image_ids:
        die(f"No image IDs found in import report: {import_report}")

    conn = BlitzGateway(username, password, host=host, port=port)
    if not conn.connect():
        die("Failed to connect to OMERO")

    try:
        if group:
            conn.setGroupForSession(group)

        chosen_image = None
        chosen_name = None

        # Prefer the main image and avoid label/macro images
        for image_id in image_ids:
            img = conn.getObject("Image", image_id)
            if img is None:
                continue
            name = img.getName() or ""
            lowered = name.lower()
            if "label image" in lowered or "macro image" in lowered:
                continue
            chosen_image = img
            chosen_name = name
            break

        # Fallback to first available image if needed
        if chosen_image is None:
            for image_id in image_ids:
                img = conn.getObject("Image", image_id)
                if img is not None:
                    chosen_image = img
                    chosen_name = img.getName() or ""
                    break

        if chosen_image is None:
            die(f"Could not resolve any imported image from IDs: {image_ids}")

        mimetype = "application/gzip" if csv_path.endswith(".gz") else "text/csv"

        ann = conn.createFileAnnfromLocalFile(
            csv_path,
            mimetype=mimetype,
            ns="omero.file.attachment",
        )
        chosen_image.linkAnnotation(ann)

        out = {
            "sample_id": sample_id,
            "image_id": chosen_image.getId(),
            "image_name": chosen_name,
            "file": csv_path,
            "file_annotation_id": ann.getId(),
            "status": "attached_to_image",
        }
        print(json.dumps(out, indent=2))

    finally:
        conn.close()


if __name__ == "__main__":
    main()