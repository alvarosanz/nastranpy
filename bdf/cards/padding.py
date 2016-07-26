from itertools import count


class Padding(object):

    def __init__(self, first_field, columns):
        self.first_field = first_field
        self.columns = columns

    def padded(self, fields):
        fields = iter(fields)
        padded_fields = list()

        for index in count(0):

            if index >= self.first_field and ((index - 1) % 8 + 1) in self.columns:
                padded_fields.append('')
            else:

                try:
                    padded_fields.append(next(fields))
                except StopIteration:
                    break

        return padded_fields

    def unpadded(self, fields):
        return [field for index, field in enumerate(fields) if not
                (index >= self.first_field and ((index - 1) % 8 + 1) in self.columns)]
