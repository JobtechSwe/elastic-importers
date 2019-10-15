import itertools as IT
import logging
from bs4 import BeautifulSoup

logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.INFO)

log = logging.getLogger(__name__)


def log_import_metrics(log, importer_name, items_count):
    json_log_msg = {'importer': importer_name, 'imported_count': items_count}
    log.info(json_log_msg)


def grouper(n, iterable):
    iterable = iter(iterable)
    return iter(lambda: list(IT.islice(iterable, n)), [])


tags_display_block = ['p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']

def clean_html(text):
    if text is None:
        cleaned_text = ''
    else:
        soup = BeautifulSoup(text, 'html.parser')
        # Remove all script-tags
        [s.extract() for s in soup('script')]

        for tag_name in tags_display_block:
            _add_linebreak_for_tag_name(tag_name, '\n', '\n', soup)

        _add_linebreak_for_tag_name('li', '', '\n', soup)
        _add_linebreak_for_tag_name('br', '', '\n', soup)

        cleaned_text = soup.get_text()

        cleaned_text = cleaned_text.strip()

    return cleaned_text

def _add_linebreak_for_tag_name(tag_name, replacement_before, replacement_after, soup):
    parent_tags = soup.find_all(tag_name, recursive=True)

    for tag in parent_tags:
        if replacement_before:
            previous_sibling = tag.find_previous_sibling()

            if not previous_sibling or \
                    (previous_sibling and previous_sibling.name not in tags_display_block):
                tag.insert_before(replacement_before)
        if replacement_after:
            tag.insert_after(replacement_after)