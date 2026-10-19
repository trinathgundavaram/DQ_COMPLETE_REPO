"""
db/nas_store.py
---------------
Read-only access to files on a NAS (SMB share), for FileAdapter. The path
parsing and SMB session logic follow the compliance framework's
NasFileStore (framework repo, feature/framework-package,
airflow/compliance_framework/adapters.py + db.py), reduced to what a
rules run needs: download one file to local disk.

NAS_HOST / NAS_USER / NAS_PASSWORD / NAS_PORT are read from the process
environment -- gre_rules_task.py sets them from an Airflow Connection.
"""
import os
import shutil
from urllib.parse import urlparse


def is_nas_uri(location: str) -> bool:
    text = (location or "").strip().replace("\\", "/")
    scheme = urlparse(text).scheme
    return text.startswith("//") or scheme in ("smb", "nas")


def parse_uri(uri: str) -> tuple:
    """
    \\\\server\\share\\folder, //server/share/folder or smb://server/share/folder
        -> ('server/share', 'folder/')
    nas://share/folder -> ('share', 'folder/') on the server of the NAS connection.
    """
    text = (uri or "").strip().replace("\\", "/")
    p = urlparse(text)
    if text.startswith("//") or p.scheme == "smb":
        share, _, folder = p.path.lstrip("/").partition("/")
        if not p.netloc or not share:
            raise ValueError(f"NAS path {uri!r} must name a server and a share")
        share = f"{p.netloc.lower()}/{share.lower()}"
    elif p.scheme == "nas" and p.netloc:
        share, folder = p.netloc.lower(), p.path
    else:
        raise ValueError(f"unsupported NAS location {uri!r}: use \\\\server\\share\\folder or nas://share/folder")
    folder = "/".join(x for x in folder.split("/") if x)
    return share, folder + "/" if folder else ""


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required env var '{key}' is not set (NAS connection).")
    return val


def nas_settings() -> dict:
    """SMB server and credentials of the NAS connection."""
    return {"server": _require("NAS_HOST").lower(), "username": _require("NAS_USER"),
            "password": _require("NAS_PASSWORD"), "port": int(os.getenv("NAS_PORT") or 445)}


def smb_client():
    try:
        import smbclient
    except ImportError:
        raise ImportError("smbprotocol is required for NAS file paths. Install with: pip install smbprotocol")
    return smbclient


def download(folder_uri: str, filename: str, dest: str) -> str:
    """Copy <folder_uri>/<filename> from the NAS to the local path `dest`."""
    share, folder = parse_uri(folder_uri)
    settings = nas_settings()
    server, _, share_name = share.rpartition("/")
    parts = [p for p in f"{folder}{filename}".split("/") if p]
    if not share_name or ".." in parts:
        raise ValueError(f"invalid NAS path {folder_uri!r}/{filename!r}")
    unc = "\\\\" + "\\".join([server or settings["server"], share_name, *parts])
    session = {k: v for k, v in settings.items() if k != "server"}
    with smb_client().open_file(unc, mode="rb", **session) as src, open(dest, "wb") as dst:
        shutil.copyfileobj(src, dst, 1024 * 1024)
    return dest
