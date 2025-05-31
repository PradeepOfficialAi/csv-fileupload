import re

def get_table_display_name(table_name):
    """
    Get a human-readable display name for a table.
    Uses a dictionary for custom mappings and falls back to formatted capitalization.
    """
    # Dictionary of custom display names
    display_name_map = {
        'glassreport': 'Glass',
        'framescutting': 'Frame',
        'casingcutting': 'Casing',
    }

    # Normalize table_name to lowercase for lookup
    table_name_lower = table_name.lower()

    # Return custom display name if defined
    if table_name_lower in display_name_map:
        return display_name_map[table_name_lower]

    # Fallback: Format the table name
    # Split camelCase or snake_case into words
    # e.g., 'casingcutting' -> 'Casing Cutting', 'work_order' -> 'Work Order'
    # Replace underscores with spaces and split on camelCase
    formatted = re.sub(r'_', ' ', table_name_lower)
    formatted = re.sub(r'([a-z])([A-Z])', r'\1 \2', formatted)
    # Capitalize each word
    formatted = ' '.join(word.capitalize() for word in formatted.split())
    return formatted