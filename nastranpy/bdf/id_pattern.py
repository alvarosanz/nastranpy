class IdPattern(object):

    def __init__(self, id_pattern):
        self.digit_patterns = list()
        self.id_min = 0
        self.id_max = 0

        for i, digit_pattern in enumerate(reversed(id_pattern)):

            if digit_pattern == '*':
                self.digit_patterns.append('0123456789')
                min_digit = 0
                max_digit = 9
            elif digit_pattern[1:2] == '-':
                self.digit_patterns.append(''.join([str(x) for x in range(int(digit_pattern[0]),
                                                                          int(digit_pattern[2]) + 1)]))
                min_digit = int(digit_pattern[0])
                max_digit = int(digit_pattern[2])
            else:
                self.digit_patterns.append(digit_pattern)
                min_digit = min(int(digit) for digit in digit_pattern)
                max_digit = max(int(digit) for digit in digit_pattern)

            self.id_min += min_digit * 10 ** i
            self.id_max += max_digit * 10 ** i

        if self.id_min == 0:
            self.id_min = 1

    def __contains__(self, value):

        if value > self.id_max or value < self.id_min:
            return False

        for digit, digit_pattern in zip(reversed(str(value)), self.digit_patterns):

            if not digit in digit_pattern:
                return False

        return True

    def __iter__(self):
        return (id for id in range(self.id_min, self.id_max + 1) if id in self)
