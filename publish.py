import os
import shutil
import subprocess


# find directory that contains this file
root = os.path.dirname(os.path.realpath(__file__))
info_dir = os.path.join(root, 'azure_kusto_notebooks.egg-info')
dist_dir = os.path.join(root, 'dist')


def clean():
    if os.path.isdir(info_dir):
        shutil.rmtree(info_dir)

    if os.path.isdir(dist_dir):
        shutil.rmtree(dist_dir)


def build():
    rc = subprocess.call('python setup.py sdist', shell=True)
    assert rc == 0
    assert os.path.isdir(os.path.join(root, 'dist'))


def upload():
    print('pypi credentials:')
    rc = subprocess.call('twine upload dist/*')
    assert rc == 0


if __name__ == "__main__":
    clean()
    build()
    upload()