import abc
import math


class Shape(abc.ABC):
    @abc.abstractmethod
    def get_area(self) -> float:
        pass


class Circle(Shape):
    def __init__(self, radius: float) -> None:
        if radius <= 0:
            raise ValueError("Radius must be positive")

        self._radius = radius

    @property
    def radius(self) -> float:
        return self._radius

    def get_area(self) -> float:
        return math.pi * self.radius**2


class Triangle(Shape):
    def __init__(
        self,
        a: float,
        b: float,
        c: float,
    ) -> None:
        if a <= 0 or b <= 0 or c <= 0:
            raise ValueError("Sides must be positive")
        if a + b <= c or a + c <= b or b + c <= a:
            raise ValueError("Invalid triangle sides")

        self._a = a
        self._b = b
        self._c = c

    @property
    def a(self) -> float:
        return self._a

    @property
    def b(self) -> float:
        return self._b

    @property
    def c(self) -> float:
        return self._c

    def get_area(self) -> float:
        s = (self.a + self.b + self.c) / 2
        return math.sqrt(s * (s - self.a) * (s - self.b) * (s - self.c))

    def is_right(self) -> bool:
        sides = sorted([self.a, self.b, self.c])
        return math.isclose(sides[0] ** 2 + sides[1] ** 2, sides[2] ** 2)


def calculate_area(shape: Shape) -> float:
    return shape.get_area()


if __name__ == "__main__":
    pass
