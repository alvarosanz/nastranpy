class FieldInfo(object):

    def __init__(self, name=None, type=None,
                 seq_type=None, length=None, subscheme=None,
                 alternate_name=None, update_grid=False,
                 optional=False, optional_flag=False,
                 other_card=False):
        self.name = name
        self.type = type
        self.seq_type = seq_type
        self.length = length
        self.subscheme = subscheme
        self.alternate_name = alternate_name
        self.update_grid = update_grid
        self.optional = optional
        self.optional_flag = optional_flag
        self.other_card = other_card
