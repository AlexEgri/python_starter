import json
import threading
import pickle
import requests
import requests.exceptions
import itertools
import sys
import re
import hashlib
import tempfile
import dateutil.parser
import subprocess
import random
import unidecode
from datetime import datetime, timedelta


# cache base URL
from __builtin__ import unicode

_CACHE_HOST = "localhost"
_CACHE_PORT = 18080

# proxies
_PROXY_USERNAME = "8741e991269347fa845ba0d753808e56"
_PROXY_PASSWORD = ""
_PROXY_HOST = "proxy.crawlera.com"
_PROXY_PORT = "8010"

# cache hash
_OUTPUT_LOCK = threading.RLock()
_USER_AGENTS = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36"
]


def _hash(key):
        """
        Hash a Python object
        :param key: Key to hash
        :return: Hashed key
        """
        if key is None:
                raise ValueError("Cannot hash NoneType")

        # hash representation of cache key
        text_bytes = repr(key).encode("utf8")
        return hashlib.sha256(text_bytes).hexdigest()


# cache
class _Cache(object):

        def __init__(self):
                self._base_url = "http://{}:{}".format(_CACHE_HOST, _CACHE_PORT)

        def with_cache(self, action, cache_key, cache_hours):

                # ignore if no cache specified
                if (cache_hours or 0) <= 0:
                        return action()

                # index key
                hashed_key = _hash(cache_key)

                # get cached item if possible
                try:
                        response = requests.get(self._base_url + "/" + hashed_key)
                        if response.status_code == 200:
                                return pickle.loads(response.content)
                except:
                        pass

                # run action
                item = action()

                # store in cache
                try:
                        expiry_utc = datetime.utcnow() + timedelta(hours=cache_hours)
                        requests.put(
                                url=self._base_url + "/" + hashed_key,
                                data=pickle.dumps(item),
                                headers={
                                        "Content-Type": "application/octet-stream",
                                        "X-Cache-Expiry": expiry_utc.isoformat() + "Z"
                                }
                        )
                except:
                        pass
                # give back item
                return item


# single cache instance
_CACHE = _Cache()


def _with_cache(action, cache_key, cache_hours):
        return _CACHE.with_cache(action, cache_key, cache_hours)


# version safe (python 2 or 3) convert to unicode
def _unicode(text, encoding="utf8"):
        if sys.version_info.major == 3:
                if isinstance(text, bytes):
                        return text.decode(encoding, errors="ignore")
                else:
                        return text
        else:
                if isinstance(text, str):
                        return text.decode(encoding, errors="ignore")
                else:
                        return text


# version safe (python 2 or 3) convert to bytes
def _bytes(text, encoding="utf8"):
        if sys.version_info.major == 3:
                if isinstance(text, str):
                        return text.encode(encoding, errors="ignore")
                else:
                        return text
        else:
                if isinstance(text, unicode):
                        return text.encode(encoding, errors="ignore")
                else:
                        return text


def _is_str(obj):
        if sys.version_info.major == 3:
                return isinstance(obj, (str, bytes))
        else:
                return isinstance(obj, (str, unicode))


# clean a string, remove extraneous data
def _clean_string(text):
        # idiot check
        if text is None:
                return None
        # respect HTML paragraph and div tags as paragraphs to extract text
        text = _unicode(text)
        text = re.sub("[<]\\s*/?\\s*?(p|div)[^>]*[>]", "\r\n\r\n", text, flags=re.IGNORECASE)

        # split into paragraphs
        cleaned_paragraphs = []
        for para in re.split("\r\n\\s+\r\n|\n\\s+\n|\r\\s+\r", text):
                # dashes, spaces, quotes and other unicode nonsense
                para = re.sub(u"[\u00A0\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a]", " ", para, re.UNICODE)
                para = re.sub(u"[\u00AD\u2010\u2011\u2012\u2013\u2014\u2015\u2212]", "-", para, re.UNICODE)
                para = re.sub(u"[\u2018\u2019]", "\'", para, re.UNICODE)
                para = re.sub(u"[\u201c\u201d]", "\"", para, re.UNICODE)
                # escape spans and span-like tags (replace with space or no-space depending on tag)
                para = re.sub(u"[<]\\s*/?\\s*?(font|span|strong|em|b|i)[^>]*[>]", "", para, flags=re.IGNORECASE | re.UNICODE)
                para = re.sub(u"[<]\\s*/?\\s*?\\w+[^>]*[>]", " ", para, flags=re.IGNORECASE | re.UNICODE)
                # normalize spaces
                cleaned_paragraphs.append(re.sub("\\s+", " ", para).strip())
        # join paragraphs back together
        return "\r\n\r\n".join(s for s in cleaned_paragraphs if s != "")


# recursively go through a dictionary and clean / copy string
def _copy_clean(json_obj):

        if isinstance(json_obj, list):
                cleaned_source = [_copy_clean(obj) for obj in json_obj]
                return [x for x in cleaned_source if x not in ["", None]]

        elif isinstance(json_obj, dict):
                cleaned_source = [(_clean_string(key), _copy_clean(value)) for key, value in json_obj.items()]
                return {k: v for k, v in cleaned_source if v not in ["", None]}

        elif _is_str(json_obj):
                return _clean_string(json_obj)

        else:
                return json_obj


def _check_imported_source_json(json_original):
        # so we can change at any time, required fields indicated with a "*"
        doc_keys = [
                "_meta",
                "name",
                "aka",
                "types",
                "fields",
                "associates"
        ]
        meta_keys = [
                "id",
                "entity_type",
                "listing_started_utc",
                "listing_ended_utc"
        ]
        entity_types = ["person", "organisation", "company", "vessel", "aircraft", "unknown"]
        source_types = ["sanction", "pep", "warning", "pep-class-1", "pep-class-2", "pep-class-3", "pep-class-4"]
        # check against cleaned JSON
        json_copy = _copy_clean(json_original)

        # shortcut to check the type
        def type_check(key, obj, wanted_type):
                fail_message = "{0} must be {1} (actual {2})".format(key, repr(wanted_type), repr(type(obj)))
                if wanted_type is str:
                        assert _is_str(obj), fail_message
                else:
                        assert isinstance(obj, wanted_type), fail_message

        # check basic properties
        assert isinstance(json_copy, dict), "source document must be JSON dictionary"
        for key in json_copy.keys():
                if key not in doc_keys:
                        raise AssertionError("source document has key '{0}' which is not allowed".format(key))

        # check "meta"
        if "_meta" in json_copy:
                json_meta = json_copy["_meta"]
                # check id field
                meta_id = json_meta.get("id", None)
                assert "_meta" in json_copy, "source document requires '_meta' dictionary for metadata"
                type_check("_meta.id", meta_id, str)
                assert len(meta_id) > 0, "source document '_meta.id' field is empty"
                assert re.match("^[0-9a-zA-Z-_:;.]+$", meta_id), "source document '_meta.id' field must match [0-9a-zA-Z-_:;.]"
                assert len(meta_id) < 100, "source document '_meta.id' field must be < 100 characters"
                # check "entity type" field
                meta_type = json_meta.get("entity_type", None)
                type_check("_meta.entity_type", meta_type, str)
                entity_type_fail_message = "source document '_meta.entity_type' must be one of " + ", ".join(entity_types)
                assert meta_type in entity_types, entity_type_fail_message
                # check that there are no other meta fields present
                for key in json_copy["_meta"].keys():
                        if key not in meta_keys:
                                raise AssertionError("source document '_meta.%s' field not permitted" % key)

        # check types
        if "types" in json_copy:
                type_check("types", json_copy["types"], list)
                for type_item in json_copy["types"]:
                        if type_item not in source_types:
                                raise AssertionError("type must be one of [{0}]".format(", ".join(source_types)))

        # check name field (required)
        type_check("name", json_copy.get("name", None), str)
        assert len(re.sub("\\s", "", json_copy["name"])) > 0, "source document 'name' is empty or whitespace"

        # check the AKA field is formatted properly in main document (if it exists)
        if "aka" in json_copy:
                type_check("aka", json_copy["aka"], list)
                for aka in json_copy["aka"]:
                        type_check("aka (entry)", aka, dict)
                        assert "name" in aka, "all 'aka' entries must have a 'name' field at the minimum"
                        for key, value in aka.items():
                                type_check("aka.{0} (entry)".format(key), value, str)

        # check each display field has a name or tag at minimum
        if "fields" in json_copy:
                type_check("fields", json_copy["fields"], list)
                for field in json_copy["fields"]:
                        if "name" not in field and "tag" not in field:
                                raise AssertionError("fields entry must have 'name' or 'tag' defined")


class FetchException(BaseException):
        """Exception on Fetch resource"""
        pass


def is_debug():
        """Returns True if we're in debug mode"""
        return "--debug" in sys.argv


def emit(source_doc):
        """Emits a source document from scraper source"""
        with _OUTPUT_LOCK:
                if "--debug" not in sys.argv:
                        print(":ACCEPT:{0}".format(json.dumps(source_doc)))
                else:
                        print(json.dumps(source_doc, indent=4) + "\r\n===========")
                sys.stdout.flush()


def get_date_text(text, day_first=False):
        """
        Retrieve date text or None from random text
        :dayfirst: for ambiguous dates such as 06/08/2012 dateutil
                             will understand the date format as mm/dd/yyyy.
                             this parameter can be used to clarify if the
                             input is in dd/mm/yyyy format.
        :return: date text or None
        """
        try:
                if day_first:
                        parser_info = dateutil.parser.parserinfo(dayfirst=True)
                else:
                        parser_info = dateutil.parser.parserinfo(dayfirst=False)
                parsed_time = dateutil.parser.parse(text, parserinfo=parser_info)
                parsed_time = parsed_time.replace(tzinfo=None)
                return parsed_time.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
                return None


def fetch_string(url, num_tries=3, cache_hours=None, use_curl=True, proxy=False, verify_ssl=False, curl_args=None):
        """Fetch a URL as a string (returns byte string only)"""

        # must use "requests" if "proxy" is True
        if proxy:
                use_curl = True

        # non-curl downloader
        def download_with_requests():
                for tries in itertools.count(1):
                        try:
                                response = requests.get(url, timeout=0, verify=verify_ssl)
                                response.raise_for_status()
                                return response.content
                        except requests.exceptions.HTTPError:
                                exception = sys.exc_info()[1]
                                # try 3 times
                                if tries > num_tries:
                                        code = exception.response.status_code
                                        raise FetchException("Could not retrieve {} (HTTP {})".format(url, code))

        # attempt to download
        def download_with_curl():
                with tempfile.TemporaryFile() as temp_file:
                        # write to temp file. The "-f" flag tells curl to fail with an exit code.
                        all_arguments = [
                                "curl",
                                "-f",
                                "--retry", str(num_tries),
                                "-A", random.choice(_USER_AGENTS),
                                "--url", url
                        ]
                        # add ignore ssl instruction
                        if not verify_ssl:
                                all_arguments.append("-k")
                        # add proxy instructions
                        if proxy:
                                all_arguments.append("-x")
                                all_arguments.append(_PROXY_HOST + ":" + _PROXY_PORT)
                                all_arguments.append("-U")
                                all_arguments.append(_PROXY_USERNAME + ":" + _PROXY_PASSWORD)
                        # exit code
                        exit_code = subprocess.call(all_arguments + (curl_args or []), stdout=temp_file, stderr=temp_file)
                        # read from temp file
                        temp_file.seek(0)
                        contents = temp_file.read()
                        # make sure we're understood!
                        if exit_code == 0:
                                return contents
                        else:
                                if exit_code == 22:
                                        try:
                                                contents = 'Response: ' + contents.splitlines()[-1]
                                        except:
                                                contents = ''
                                raise FetchException("Could not retrieve {0} (return code: {1}. {2})".format(url, exit_code, contents))
        # use cache to download (if appropriate)
        if use_curl:
                download = download_with_curl
        else:
                download = download_with_requests
        return _CACHE.with_cache(download, url, cache_hours)


def check_silent(iterable):
        """Checks each document to see if it's valid, output to STDOUT
        :param iterable: an iterable of JSON documents to be checked against
        """
        # check single or multiple documents
        if isinstance(iterable, dict):
                _check_imported_source_json(iterable)
        else:
                for doc in iterable:
                        _check_imported_source_json(doc)


def check(iterable):
        """Checks each document to see if it's valid, output to STDOUT.    Can be used for live testing.
        :param iterable: an iterable of JSON documents to be checked against
        """
        if isinstance(iterable, dict):
                # check a single document
                doc = iterable
                print("")
                print(json.dumps(doc, indent=4))
                print("=============================")
                _check_imported_source_json(iterable)
        else:
                # check each document
                counter = 0
                for doc in iterable:
                        print("")
                        print(json.dumps(doc, indent=4))
                        print("=============================")
                        counter += 1
                        _check_imported_source_json(doc)
                # total
                print("")
                print("No problems found with {0} documents".format(counter))


def make_id(name):
        """Creates an id for an entity.
        :param name: a string to be converted to an id for an entity, preferably the entity's name
        """
        return re.sub(r"[^0-9a-zA-Z\-_:;\.]", "", unidecode.unidecode(name))