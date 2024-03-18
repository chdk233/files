from pcd_iot_dbx.common import Task

from urllib.parse import urlparse

def retrieve_ssl_password(task: Task,url: str) -> str:
    p_rs = urlparse(url)

    if p_rs.scheme == 'dbxsecrets':
        return task.dbutils.secrets.get(scope=p_rs.netloc,
                                        key=p_rs.path[1:])
    if p_rs.scheme == 'file':
        with open(p_rs.netloc + p_rs.path) as f:
            return f.read(256)