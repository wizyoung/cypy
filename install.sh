set -e

python3 -m pip uninstall cypy
rm -rf build && rm -rf dist && rm -rf cypy.egg-info
python3 setup.py bdist_wheel
ls dist/*whl | xargs python3 -m pip install -U
rm -rf build && rm -rf dist && rm -rf cypy.egg-info

