#!/usr/bin/env python3
"""Build a WGS84 taxi-zone centroid lookup from the official NYC TLC shapefile."""

import argparse
import csv
from pathlib import Path

import shapefile
from pyproj import CRS, Transformer


def polygon_centroid(points, parts):
    weighted_x = 0.0
    weighted_y = 0.0
    total_area = 0.0
    boundaries = list(parts) + [len(points)]

    for start, end in zip(boundaries, boundaries[1:]):
        ring = points[start:end]
        if len(ring) < 3:
            continue
        if ring[0] != ring[-1]:
            ring = [*ring, ring[0]]

        twice_area = 0.0
        centroid_x = 0.0
        centroid_y = 0.0
        for first, second in zip(ring, ring[1:]):
            cross = first[0] * second[1] - second[0] * first[1]
            twice_area += cross
            centroid_x += (first[0] + second[0]) * cross
            centroid_y += (first[1] + second[1]) * cross

        if abs(twice_area) < 1e-12:
            continue
        area = twice_area / 2.0
        weighted_x += (centroid_x / (3.0 * twice_area)) * area
        weighted_y += (centroid_y / (3.0 * twice_area)) * area
        total_area += area

    if abs(total_area) < 1e-12:
        raise ValueError("Polygon has no measurable area")
    return weighted_x / total_area, weighted_y / total_area


def build_centroids(shapefile_path: Path, output_path: Path) -> None:
    reader = shapefile.Reader(str(shapefile_path))
    projection_path = shapefile_path.with_suffix(".prj")
    source_crs = CRS.from_wkt(projection_path.read_text(encoding="utf-8"))
    transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)

    rows = []
    for shape_record in reader.iterShapeRecords():
        record = shape_record.record.as_dict()
        centroid_x, centroid_y = polygon_centroid(
            shape_record.shape.points,
            shape_record.shape.parts,
        )
        longitude, latitude = transformer.transform(centroid_x, centroid_y)
        rows.append(
            {
                "LocationID": int(record["LocationID"]),
                "Borough": record["borough"],
                "Zone": record["zone"],
                "Latitude": f"{latitude:.7f}",
                "Longitude": f"{longitude:.7f}",
            }
        )

    rows.sort(key=lambda row: row["LocationID"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["LocationID", "Borough", "Zone", "Latitude", "Longitude"],
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("shapefile", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    build_centroids(args.shapefile, args.output)


if __name__ == "__main__":
    main()
