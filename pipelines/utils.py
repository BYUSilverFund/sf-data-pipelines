from jinja2 import Template

def render_sql_file(sql_file: str, **kwargs) -> str:
    """
    Reads an SQL file, renders it using Jinja2 with given parameters.

    :param sql_file: Path to the SQL file
    :param params: Dictionary of parameters to replace in the SQL file
    :return: Parameterized string of sql query.
    """
    with open(sql_file, "r") as file:
        template = Template(file.read())

    query = template.render(**kwargs)

    return query