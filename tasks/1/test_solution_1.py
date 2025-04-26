import math

import pytest
from solution_1 import Circle, Triangle, calculate_area


class TestCircle:
    def test_circle_area(self) -> None:
        circle = Circle(1)

        assert math.isclose(circle.get_area(), math.pi)
        assert math.isclose(calculate_area(circle), math.pi)

    def test_circle_invalid_radius(self) -> None:
        with pytest.raises(ValueError):
            Circle(0)


class TestTriangle:
    def test_triangle_area(self) -> None:
        triangle = Triangle(3, 4, 5)

        assert math.isclose(triangle.get_area(), 6.0)
        assert math.isclose(calculate_area(triangle), 6.0)

    def test_triangle_invalid_sides(self) -> None:
        with pytest.raises(ValueError):
            Triangle(1, 2, 3)

    @pytest.mark.parametrize(
        "a, b, c, expected",
        [
            (3, 4, 5, True),
            (2, 2, 2, False),
        ],
    )
    def test_triangle_is_right(
        self,
        a: float,
        b: float,
        c: float,
        expected: bool,
    ) -> None:
        triangle = Triangle(a, b, c)

        assert triangle.is_right() == expected

    def test_triangle_non_positive(self) -> None:
        with pytest.raises(ValueError):
            Triangle(-1, 1, 1)
