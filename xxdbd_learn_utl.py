def dict_clean(items):
    result = {}
    for key, value in items:
        if value == "None":
            value = None
        result[key] = value
    return result
