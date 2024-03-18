from importlib.resources import files

def where(name: str) -> str:

    return str(files("pcd_iot_dbx.certs").joinpath(name))