def von_mises_2D(sxx, syy, sxy, thickness=None):
    value = (sxx ** 2 + syy ** 2 - sxx * syy + 3 * sxy ** 2) ** 0.5

    if thickness is None:
        return value
    else:
        return value / thickness


def max_ppal_2D(sxx, syy, sxy, thickness=None):
    value = (sxx + syy) / 2 + (((sxx - syy) / 2) ** 2 + sxy ** 2) ** 0.5

    if thickness is None:
        return value
    else:
        return value / thickness


def min_ppal_2D(sxx, syy, sxy, thickness=None):
    value = (sxx + syy) / 2 - (((sxx - syy) / 2) ** 2 + sxy ** 2) ** 0.5

    if thickness is None:
        return value
    else:
        return value / thickness


def max_shear_2D(sxx, syy, sxy, thickness=None):
    value = (((sxx - syy) / 2) ** 2 + sxy ** 2) ** 0.5

    if thickness is None:
        return value
    else:
        return value / thickness


query_functions = {
    'VON MISES (2D)': von_mises_2D,
    'MAX PPAL (2D)': max_ppal_2D,
    'MIN PPAL (2D)': min_ppal_2D,
    'MAX SHEAR (2D)': max_shear_2D,
}

query_templates = {

}
