import unittest
import re

from converters import tag2html


class TestTag2Html(unittest.TestCase):

    def test_basic_color_conversion(self):
        input_string = "[color=#123123]This text should be colored[/color]"
        expected_output = "<font color=#123123>This text should be colored</font>"
        self.assertEqual(tag2html(input_string), expected_output)

    def test_hex_color_conversion(self):
        input_string = "[color=#fff123]This text should be colored[/color]"
        expected_output = "<font color=#fff123>This text should be colored</font>"
        self.assertEqual(tag2html(input_string), expected_output)

    def test_multiple_color_conversion(self):
        input_string = "[color=#123567]This text should be colored[/color] [color=#000000]This text should be black[/color]"
        expected_output = "<font color=#123567>This text should be colored</font> <font color=#000000>This text should be black</font>"
        self.assertEqual(tag2html(input_string), expected_output)

    def test_color_conversion_with_other_tags(self):
        input_string = "[b][color=#123567]This text should be colored[/color][/b]"
        expected_output = (
            "<b><font color=#123567>This text should be colored</font></b>"
        )
        self.assertEqual(tag2html(input_string), expected_output)

    def test_color_conversion_with_nested_tags(self):
        input_string = (
            "[b][color=#123567][i]This text should be colored[/i][/color][/b]"
        )
        expected_output = (
            "<b><font color=#123567><i>This text should be colored</i></font></b>"
        )
        self.assertEqual(tag2html(input_string), expected_output)

    def test_color_conversion_with_invalid_color_code(self):
        input_string = "[color=#invalid]This text should be colored[/color]"
        expected_output = "[color=#invalid]This text should be colored[/color]"
        self.assertEqual(tag2html(input_string), expected_output)
