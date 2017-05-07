from itertools import count


class Padding(object):

    def __init__(self, first_field=0, columns=None):

        if columns is None:
            columns = set()

        self.first_field = first_field
        self.columns = columns
        self.first_column = min({1, 2, 3, 4, 5, 6, 7, 8} - columns)

    def padded(self, fields):
        padded_fields = list()
        write_field = True

        for index in count(0):
            column = (index - 1) % 8 + 1

            if index >= self.first_field and column in self.columns:
                padded_fields.append('')
            else:

                if write_field:

                    try:
                        field, field_info, is_optional_flag = next(fields)

                        if is_optional_flag:
                            write_field = False
                    except StopIteration:
                        break

                if not write_field and column == self.first_column:
                    write_field = True

                if write_field:
                    padded_fields.append(field)
                else:
                    padded_fields.append('')

        return padded_fields

    def unpadded(self, fields):
        return [field for index, field in enumerate(fields) if not
                (index >= self.first_field and ((index - 1) % 8 + 1) in self.columns)]
