import urllib.request
import sys

OUT = 'Win64OpenSSL.exe'

URL_STR = 'https://slproweb.com/download/Win64OpenSSL-%s.exe'

VERSION_MAJOR = 1
VERSION_MINOR = 0
VERSION_PATCH = 2
VERSION_SUFFIX = 'j'
VERSION_STR = '%d_%d_%d%s'

TRY_COUNT = 4


def main():
    for patch in range(VERSION_PATCH, TRY_COUNT):
        for suffix in range(TRY_COUNT):
            if patch == VERSION_PATCH:
                s = VERSION_SUFFIX
            else:
                s = 'a'
            s = chr(ord(s) + suffix)
            ver = VERSION_STR % (VERSION_MAJOR, VERSION_MINOR, patch, s)
            url = URL_STR % ver
            try:
                with urllib.request.urlopen(url) as res:
                    if res.getcode() == 200:
                        with open(OUT, 'wb') as out:
                            out.write(res.read())
                            print('successfully downloaded from ' + url)
                            return 0
            except urllib.error.HTTPError:
                pass
            print('failed to download from ' + url, file=sys.stderr)
    print('could not download openssl', file=sys.stderr)
    return 1

if __name__ == '__main__':
    sys.exit(main())
