"""Cookbooks"""
import argparse


__title__ = __doc__


class ArgparseFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    """Custom argparse formatter for cookbooks."""
