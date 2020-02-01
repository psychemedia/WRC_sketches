# ---
# jupyter:
#   jupytext:
#     formats: py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Utility Functions

# + tags=["active-ipynb"]
# # %pip install flake8 pycodestyle_magic
#
# # Install additional style convention rules
# #%pip install pep8-naming flake8-bugbear flake8-docstrings flake8-builtins
#
# # Provide additional conventions for pandas code: https://github.com/deppen8/pandas-vet
# # %pip install pandas-vet
#
# %load_ext pycodestyle_magic
#
# %flake8_on --ignore D100
# -

from pandas import DataFrame


# Should we handle open ended paths?
#
# Fragment to navigate paths:
#
# ```
# paths=[]
# parts=attr.split('.')
# for i, _ in enumerate(parts):
#     if i:
#         paths.append('.'.join(parts[:-i]))
# paths.reverse()
# ```

def _isnull(obj):
    """Check an object is null."""
    if isinstance(obj, DataFrame):
        return obj.empty
    elif isinstance(obj, str) and obj.lower() == 'null':
        return True
    elif obj:
        return False
    return True


# +
def _notnull(obj):
    """Check an object is not null."""
    return not _isnull(obj)


def _checkattr(obj, attr):
    """Check an object exists and is set to a non-null value."""
    # TO DO  - support attributes done a path, checking each step in turn

    if hasattr(obj, attr):
        objattr = getattr(obj, attr)
        return _notnull(objattr)

    return False

# + tags=["active-ipynb"]
# assert _isnull('')
# assert _isnull(None)
# assert _isnull({})
# assert _isnull([])
# assert _isnull(DataFrame())

# +
# TO DO - this should go into a general utils package


def _jsInt(val):
    """
    Ensure we have a JSON serialisable value for an int.

    This defends against non-JSON-serialisable np.int64.
    """
    if val:
        try:
            val = int(val)
        except:
            return None
    else:
        val = None

    return val


# + tags=["active-ipynb"]
# assert _jsInt(1)==1
# assert _jsInt('a') is None
# assert _jsInt('') is None
# assert _jsInt(None) is None
# -

def listify(item):
    """Ensure an item is a list."""
    return item if isinstance(item, list) else [item]

# + tags=["active-ipynb"]
# assert listify(None) == [None]
# assert listify(1) == [1]
# assert listify('1') == ['1']
# assert listify([1, 2]) == [1, 2]
# assert listify((1, 2)) == [(1, 2)]
# assert listify({'a': 1}) == [{'a': 1}]
# -

