"""Ganeti Cookbooks"""
__title__ = __doc__

from spicerack.ganeti import CLUSTERS_AND_ROWS


def get_locations():
    """Generate short location names with datacenter and row for all Ganeti clusters."""
    locations = {}
    for cluster, rows in CLUSTERS_AND_ROWS.items():
        dc = cluster.split('.')[2]
        if len(rows) == 1:
            locations[dc] = (cluster, rows[0], dc)
        else:
            for row in rows:
                locations['{dc}_{row}'.format(dc=dc, row=row)] = (cluster, row, dc)

    return locations
