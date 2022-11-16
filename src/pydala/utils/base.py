import random
import string


def get_ddb_sort_str(sort_by: str | list, ascending: bool | list | None = None) -> str:
    ascending = ascending or True
    if isinstance(sort_by, list):

        if isinstance(ascending, bool):
            ascending = [ascending] * len(sort_by)

        sort_by_ddb = [
            f"{col} ASC" if asc else f"{col} DESC"
            for col, asc in zip(sort_by, ascending)
        ]
        sort_by_ddb = ",".join(sort_by_ddb)

    else:
        sort_by_ddb = sort_by + " ASC" if ascending else sort_by + " DESC"

    return sort_by_ddb


def random_id():
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.choices(alphabet, k=8))


def convert_size_unit(size, unit="MB"):
    if unit == "B":
        return round(size, 1)
    elif unit == "KB":
        return round(size / 1024, 1)
    elif unit == "MB":
        return round(size / 1024**2, 1)
    elif unit == "GB":
        return round(size / 1024**3, 1)
    elif unit == "TB":
        return round(size / 1024**4, 1)
    elif unit == "PB":
        return round(size / 1024**5, 1)



class NestedDictReplacer:
    def __init__(self, d: dict) -> None:
        self._d = d

    def _dict_replace_value(self, d: dict, old: str | None, new: str | None) -> dict:
        x = {}
        for k, v in d.items():
            if isinstance(v, dict):
                v = self._dict_replace_value(v, old, new)
            elif isinstance(v, list):
                v = self._list_replace_value(v, old, new)
            else:
                v = v if v != old else new
            x[k] = v
        return x

    def _list_replace_value(self, l: list, old: str | None, new: str | None) -> list:
        x = []
        for e in l:
            if isinstance(e, list):
                e = self._list_replace_value(e, old, new)
            elif isinstance(e, dict):
                e = self._dict_replace_value(e, old, new)
            else:
                e = e if e != old else new
            x.append(e)
        return x

    def replace(self, old: str | None, new: str | None) -> dict:
        d = self._d
        return self._dict_replace_value(d, old, new)
