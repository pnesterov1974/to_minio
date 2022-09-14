from pprint import pprint

import pandas as pd

def list_minio_objects(minio_iter_data):
    l = []
    for r in minio_iter_data:
        d = {}
        d['content_type'] = r.content_type
        d['etag'] = r.etag
        d['is_delete_marker'] = r.is_delete_marker
        d['is_dir'] = r.is_dir
        d['is_latest'] = r.is_latest
        d['last_modified'] = r.last_modified
        d['metadata'] = r.metadata
        d['object_name'] = r.object_name
        d['owner_id'] = r.owner_id
        d['owner_name'] = r.owner_name
        d['size'] = r.size
        d['storage_class'] = r.storage_class
        d['version_id'] = r.version_id
        l.append(d)
    return l

def pd_minio_objects(minio_iter_data):
    ld = list_minio_objects(minio_iter_data)
    first_r = ld[0]
    #d = {}
    d = {k: list() for k in first_r}
    #for k in first:
    #    d[k] = list()
    for r in ld:
        for k, v in r.items():
            d[k].append(v)
    df = pd.DataFrame(data=d)
    return df




# ---------------------------------------------------------------------------------------
if __name__ == '__main__': pass