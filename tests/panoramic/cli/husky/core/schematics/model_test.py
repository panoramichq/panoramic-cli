from enum import Enum

from schematics.types import BooleanType, FloatType, IntType, StringType, UnionType

from panoramic.cli.husky.core.schematics.model import (
    EnumType,
    SchematicsModel,
    UnionNoConversionType,
)
from tests.panoramic.cli.husky.test.test_base import BaseTest


class TestEnumType(BaseTest):
    def test_enum_type(self):
        class MyEnum(Enum):
            a = 1
            b = 2

        class ModelWithEnum(SchematicsModel):
            e = EnumType(MyEnum)
            default = EnumType(MyEnum, default=MyEnum.b)

        instance = ModelWithEnum(dict(e=1))
        assert instance.e == MyEnum.a
        assert instance.default == MyEnum.b


class TestUnionType(BaseTest):
    def test_regular_union_type(self):
        """
        Test that shows tricky behaviour of regular union type
        """
        union_def = UnionType((IntType, FloatType, StringType, BooleanType))

        self.assertEqual(union_def('464'), 464)  # Notice the conversion! It depends on order of types in the construct.
        self.assertEqual(union_def(464), 464)

    def test_union_no_conversion_type(self):
        """
        This test shows no conversion on our union type.
        """
        union_def = UnionNoConversionType((IntType, FloatType, StringType, BooleanType))

        test_values = ['1', 1, 1.8, '1.8', '', True, False, 0]
        for test_val in test_values:
            self.assertEqual(union_def(test_val), test_val)
