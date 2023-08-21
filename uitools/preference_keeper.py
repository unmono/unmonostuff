import json
import os
from pathlib import Path
from typing import Callable, Any


class JSONPreferencesKeeper:
    """
    Provides methods to store attributes of subclass in JSON file, retrieve and set them up.

    Usage:
    ------
    define_prefs('attribute', 'names', 'you', 'want', 'to', 'store') - after subclass
    initialization. Optional path of the file to use can be provided.

    save_prefs() - to store preferences

    set_prefs() - to retrieve and set those attributes

    Serialization:
    --------------
    To deal with special instances, that should or must be converted(e.g. unserializable
    instances), override convert_prefs_functions method. See its docs for details.
    """
    _preferences: tuple[str]
    _preferences_file: Path

    def serilaization_functions(self) -> dict[any, tuple[Callable, Callable]]:
        """
        Maps defined types that needed to store and set up in a special way to
        functions that do it. Each entry of returned dict is:

        type: (function1, function2)

        function1(value) is used to convert attribute value to store.

        function2(attr_name, instance_itself, value) is used to deconvert value
        and set it.

        For example, if it's needed to store strings in the uppercase, but use them
        in lowercase, just because you can, one entry of this dictionary would be:
        :return: {str: (lambda value: value.upper(), lambda name, obj, value: setattr(self, name, value.lower()}
        """
        return {
            # str: (lambda value: value.upper(), lambda name, obj, value: setattr(self, name, value.lower()
        }

    def convert_prefs(self, value) -> Any:
        """
        Uses first serialization function if defined to convert values of
        attributes to store them in JSON file. If value has a type not listed in
        dictionary, it will be returned as is.

        :param value: value to store
        :return: converted value
        """
        sf = self.serilaization_functions()
        for pref_type, converters in sf.items():
            if isinstance(value, pref_type):
                return converters[0](value)
        return value

    def deconvert_prefs(self, attr_name: str, attr_instance: Any, stored_value: str) -> None:
        """
        Uses second serialization function to deconvert stored values
        and set them as values of those attributes in subclass.

        :param attr_name: name of attribute that we want ot set value to.
        :param attr_instance: instance of the attribute for the cases when we need it
        to set up the value. For example tkinter.Variable.get or .set methods.
        :param stored_value: value stored in JSON file
        """
        sf = self.serilaization_functions()
        for pref_type, funcs in sf.items():
            if isinstance(attr_instance, pref_type):
                funcs[1](attr_name, attr_instance, stored_value)
                return

    def save_prefs(self) -> None:
        """
        Creates default or defined JSON file if it doesn't exist.
        Converts defined attributes values if needed and saves them in this file.
        If attribute is None or doesn't exist it will be ignored
        """
        preferences_to_save = {}
        for p in self._preferences:
            value = getattr(self, p)
            if value is None or not hasattr(self, p):
                continue
            preferences_to_save[p] = self.convert_prefs(value)

        self._preferences_file.touch()
        with self._preferences_file.open(mode='w') as f:
            json.dump(preferences_to_save, f)

    def set_prefs(self) -> None:
        """
        Loads saved values from JSON file and sets them to according attributes.
        Sets only defined attributes.
        To set values uses deconvert_attributes method.
        """

        # If file doesn't exist, do nothing
        if not self._preferences_file.exists():
            return
        with self._preferences_file.open(mode='r') as f:
            saved_preferences = json.load(f)
        for p in self._preferences:
            if p not in saved_preferences:
                continue
            # For some deconvert functions we may need an actual instance of an attribute
            attr_instance = getattr(self, p)
            self.deconvert_prefs(p, attr_instance, saved_preferences[p])

    def define_prefs(self, *args: str, preferences_file: str | os.PathLike = 'preferences.json') -> None:
        """
        Saves provided attribute names and save them as tuple of strings.

        :param args: names of attributes to store
        :param preferences_file: specific file to store values
        :raise: AttributeError if one of the provided args isn't
                a string or there is no such attribute in the subclass
        """
        for arg in args:
            if not isinstance(arg, str):
                raise AttributeError('All attribute names should be strings')
            if not hasattr(self, arg):
                raise AttributeError(f'No such attribute \'{arg}\'')

        self._preferences = args
        self._preferences_file = Path(preferences_file)

        # todo: check file exists, is available
