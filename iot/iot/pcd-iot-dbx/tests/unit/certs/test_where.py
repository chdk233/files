from pcd_iot_dbx import certs
import os


def test_where():
    assert os.path.dirname(certs.__file__) + '/test' == certs.where('test')
