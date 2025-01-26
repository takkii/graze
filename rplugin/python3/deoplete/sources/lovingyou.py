from dask.dataframe.io.io import from_pandas
import gc
import multiprocessing
import os
import pandas as pd
import random
import re
import sys
import traceback
import warnings

from deoplete.source.base import Base
from operator import itemgetter
from typing import Optional

warnings.filterwarnings('ignore')


# GitHub:
class Source(Base):

    def __init__(self, vim):
        super().__init__(vim)
        self.name: Optional[str] = 'lovingyou'
        self.filetypes: Optional[list] = ['javascript','typescript']
        mark_synbol: Optional[str] = '[pandas]'
        self.mark: Optional[str]  = str(mark_synbol)
        ruby_match: Optional[list] = [r'\.[a-zA-Z0-9_?!]*|[a-zA-Z]\w*::\w*']
        slash_none: Optional[list] = [r'[;/[^¥/]\*/]']
        self.input_pattern: Optional[str] = '|'.join(ruby_match + slash_none)
        self.rank: Optional[int] = 500

    def get_complete_position(self, context):
        m = re.search('[a-zA-Z0-9_?!]*$', context['input'])
        return m.start() if m else -1

    def gather_candidates(self, context):
        try:
            # It doesn't support python4 yet.
            py_mj: Optional[int] = sys.version_info[0]
            py_mi: Optional[int] = sys.version_info[1]

            # 3.5 and higher, 4.x or less,python version is required.
            if (py_mj == 3 and py_mi > 4) or (py_mj < 4):

                # Settings, vim-plug | neovim path is true/false folder search.
                neo_f: Optional[str] = '~/.neovim/plugged/lovingyou/dict/'
                neo_t = '~/.neovim/plugged/lovingyou/dict/javascript.txt'

                # Settings, vim-plug | vim path is true/false folder search.
                vim_f: Optional[str] = '~/.vim/plugged/lovingyou/dict/'
                vim_t = '~/.vim/plugged/lovingyou/dict/javascript.txt'

                # Settings, $HOME/dict path is true/false folder search.
                loc_f: Optional[str] = '~/dict/'
                loc_t: Optional[str] = '~/dict/javascript.txt'

                # Home Folder, Set the dictionary.
                if os.path.exists(os.path.expanduser(loc_f)):

                    # Get Receiver/lovingyou behavior.
                    with open(os.path.expanduser(loc_t)) as r_meth:
                        # pandas and dask
                        index_php: Optional[list] = list(r_meth.readlines())
                        pd_php = pd.Series(index_php)
                        st_r = pd_php.sort_index()
                        ddf = from_pandas(
                            data=st_r, npartitions=multiprocessing.cpu_count())
                        data_array = ddf.to_dask_array(lengths=True)
                        data = data_array.compute()
                        data_py: Optional[list] = [s.rstrip() for s in data]

                        # sorted and itemgetter
                        sorted(data_py, key=itemgetter(0))
                        return data_py

                # Neovim Folder, Set the dictionary.
                elif os.path.exists(os.path.expanduser(neo_f)):

                    # Get Receiver/lovingyou behavior.
                    with open(os.path.expanduser(neo_t)) as r_meth:
                        # pandas and dask
                        neo_php: Optional[list] = list(r_meth.readlines())
                        pd_php = pd.Series(neo_php)
                        st_r = pd_php.sort_index()
                        ddf = from_pandas(
                            data=st_r, npartitions=multiprocessing.cpu_count())
                        data_array = ddf.to_dask_array(lengths=True)
                        data = data_array.compute()
                        neo_py: Optional[list] = [s.rstrip() for s in data]

                        # sort and itemgetter
                        neo_py.sort(key=itemgetter(0))
                        return neo_py

                # Vim Folder, Set the dictionary.
                elif os.path.exists(os.path.expanduser(vim_f)):

                    # Get Receiver/lovingyou behavior.
                    with open(os.path.expanduser(vim_t)) as r_meth:
                        # pandas and dask
                        vim_php: Optional[list] = list(r_meth.readlines())
                        pd_php = pd.Series(vim_php)
                        st_r = pd_php.sort_index()
                        ddf = from_pandas(
                            data=st_r, npartitions=multiprocessing.cpu_count())
                        data_array = ddf.to_dask_array(lengths=True)
                        data = data_array.compute()
                        vim_py: Optional[list] = [s.rstrip() for s in data]

                        # sort and itemgetter
                        vim_py.sort(key=itemgetter(0))
                        return vim_py

                # Config Folder not found.
                else:
                    raise ValueError("None, Please Check the Config Folder")

            # Python_VERSION: 3.5 or higher and 4.x or less.
            else:
                raise ValueError("VERSION: 3.5 and higher, 4.x or less")

        # TraceBack.
        except Exception:
            # Load/Create LogFile.
            except_folder: Optional[str] = '~/lovingyou_log/'
            except_file: Optional[str] = '~/lovingyou_log/lovingyou_error.log'

            # Load the dictionary.
            if os.path.isdir(os.path.expanduser(except_folder)):
                with open(os.path.expanduser(except_file), 'a') as log_py:
                    traceback.print_exc(file=log_py)

                    # throw except.
                    raise RuntimeError from None

            # skl_str Folder not found.
            else:
                raise ValueError("None, Please Check the lovingyou Folder.")

        # Once Exec.
        finally:
            # GC collection.
            gc.collect()
