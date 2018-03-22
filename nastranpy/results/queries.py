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

def stress_2D(value, thickness):
    return value / thickness


query_functions = {
    'ELEMENT FORCES - QUAD4': {
        'SX': [stress_2D, ('NX', 'THK')],
        'SY': [stress_2D, ('NY', 'THK')],
        'SXY': [stress_2D, ('NXY', 'THK')],
        'VON_MISES': [von_mises_2D, ('NX', 'NY', 'NXY', 'THK')],
        'MAX_PPAL': [max_ppal_2D, ('NX', 'NY', 'NXY', 'THK')],
        'MIN_PPAL': [min_ppal_2D, ('NX', 'NY', 'NXY', 'THK')],
        'MAX_SHEAR': [max_shear_2D, ('NX', 'NY', 'NXY', 'THK')],
    },
    'ELEMENT FORCES - TRIA3': {
        'SX': [stress_2D, ('NX', 'THK')],
        'SY': [stress_2D, ('NY', 'THK')],
        'SXY': [stress_2D, ('NXY', 'THK')],
        'VON_MISES': [von_mises_2D, ('NX', 'NY', 'NXY', 'THK')],
        'MAX_PPAL': [max_ppal_2D, ('NX', 'NY', 'NXY', 'THK')],
        'MIN_PPAL': [min_ppal_2D, ('NX', 'NY', 'NXY', 'THK')],
        'MAX_SHEAR': [max_shear_2D, ('NX', 'NY', 'NXY', 'THK')],
    },
}
