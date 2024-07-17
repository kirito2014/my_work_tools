from distutils.core import setup
import py2exe

setup(
    windows=[{'script': 'main_script.py'}],
    options={
        'py2exe': {
            'includes': ['pandas', 'openpyxl', 'tkinter','PIL'],
        }
    }
)
