import os.path as osp
import sys


CODE_DIR = osp.dirname(__file__)
if CODE_DIR not in sys.path:
    sys.path.append(CODE_DIR)
from exprmngr.expr_mngr import ExprMngr

__name__ = [
    ExprMngr
]