import dataclasses


@dataclasses.dataclass(frozen=True, slots=True)
class BaseDataEntity:
    """
    Base class for all data entities.

    Examples
    --------
    Create a new data entity by inheriting from this class, like this:

    >>> @dataclasses.dataclass(frozen=True, slots=True)
    ... class MyEntity(BaseDataEntity):
    ...     some_field: str
    ...     some_other_field: float

    """
