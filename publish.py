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
    cmd = 'python setup.py sdist'
    rc = subprocess.call(cmd, shell=True)
    assert rc == 0, cmd
    assert os.path.isdir(os.path.join(root, 'dist'))


def upload():
    print('pypi credentials:')
    cmd = 'twine upload dist/*'
    rc = subprocess.call(cmd, shell=True)
    assert rc == 0, cmd


if __name__ == "__main__":
    clean()
    build()
    upload()