from pcd_iot_dbx.util import secrets
from pcd_iot_dbx.common import Task

import pytest

@pytest.fixture(scope="session")
def task():
    
    class test_task(Task):
        def launch(self):
            pass

    yield test_task()

@pytest.fixture(scope="session")
def file():
    
    path = 'data/sample_secret_file'

    with open(path,'w') as f:
        f.write('sample_password')

    yield path



def test_dbxsecrets(task):

    pw = secrets.retrieve_ssl_password(task,'dbxsecrets://myscope/mykey')

    assert pw == 'myscopemykey'

def test_file(file):

    pw = secrets.retrieve_ssl_password(task,f'file://{file}')

    assert pw == 'sample_password'