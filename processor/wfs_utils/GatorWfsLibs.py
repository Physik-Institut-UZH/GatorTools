import sys
from os import path

import json
import ast
from pathlib import Path

from typing import Union
from typing import Optional
import copy
import numpy as np
from scipy.signal import convolve
from scipy.ndimage import label
import random
import pandas as pd


def BslnCorr(wfs_storage, bslns_meth, flip_wf=False):
    #This function returns the waveforms shift to zero baseline and flipped for the pulse sign
    wfs_dict = wfs_storage.getWfs()
    df = wfs_storage.getDf()

    wfs_proc = dict()
    if bslns_meth=='mean':
        for wf_name in wfs_dict:
            wfs_proc[wf_name] = wfs_dict[wf_name] - (df[wf_name+'_bslns_mean']).to_numpy().reshape(-1,1)
        #
    elif bslns_meth=='median':
        for wf_name in wfs_dict:
            wfs_proc[wf_name] = wfs_dict[wf_name] - (df[wf_name+'_bslns_mean']).to_numpy().reshape(-1,1)
        #
    else:
        raise ValueError(f'Wrong value of the method ({bslns_meth}). Only "mean" and "median" are allowed values.')
    #
        
    if flip_wf:
        for wf_name in wfs_proc:
            wfs_proc[wf_name] = -1.0*wfs_proc[wf_name]
        #
    #
    
    return wfs_proc
#

def trapezoidalFilt(arr: np.array, shape_time: int, tau: float, flat_top: int):
    arr = np.copy(arr)
    
    is_1d = (arr.ndim == 1)
    if is_1d:
        arr = arr[None, :]   # make (1, Nsamps) for unified code
    #
    
    n_wfs, n_samps = arr.shape

    #Aliases to reduce the verbosity of the equations below
    w = shape_time
    g = flat_top
    
    sn = 0.0
    pn = 0.0

    trapezoid = np.zeros_like(arr)

    a = np.exp(1./tau)-1

    for n in range(n_samps):
        dlk = arr[:, n].copy()   # make local copy
        if (n-w)>=0:
            dlk -= arr[:,n-w]
        if (n-w-g)>=0:
            dlk -= arr[:,n-w-g]
        if (n-2*w-g)>=0:
            dlk += arr[:,n-2*w-g]
        #
        if (n==0):
            pn = dlk
            #sn = pn + dlk*tau
            sn = pn + dlk/a
        else:
            pn += dlk
            #sn += pn + dlk*tau
            sn += pn + dlk/a
        #
        
        trapezoid[:,n] = sn/tau/w
    #
    if is_1d:
        trapezoid = trapezoid.flatten()
    #
    return trapezoid
#

def gaussian_filter(wfs:np.array, sigma:float, kernel_half_width:int, derivative: bool = False):
    """
    Apply Gaussian smoothing (or its derivative) to 1D or 2D waveform arrays.

    Parameters:
        wfs : np.ndarray
            1D array (samples,) or 2D array (n_waveforms, n_samples)
        sigma : float
            Standard deviation of the Gaussian kernel
        kernel_half_width : int
            Half-width of the kernel in samples
        derivative : bool, optional
            If True, apply derivative-of-Gaussian filter (default False = smoothing only)

    Returns:
        filtered : np.ndarray
            Filtered waveform(s), same shape as input
    """
    wfs = wfs.copy()
    if np.ndim(wfs)==1:
        wfs = wfs[None,:]
        is_1d = True
    else:
        is_1d = False
    #
    
    x = np.arange(-kernel_half_width, kernel_half_width + 1)
    
    # Gaussian kernel
    gaussian = np.exp(-x**2 / (2 * sigma**2))
    
    if not derivative:
        kernel = gaussian/gaussian.sum()
    else:
        # derivative of Gaussian
        kernel = -x * gaussian / (sigma**2)
        kernel -= kernel.mean()  # zero mean for stability
    #
    
    # Convolve waveform with derivative kernel
    smooth_wfs = np.array([convolve(wf, kernel, mode='same') for wf in wfs])
    if is_1d:
        smooth_wfs = smooth_wfs.flatten()
    
    return smooth_wfs
#

def gauss_filters(wfs:np.array, sigma:float, kernel_half_width:int):
    dwfs = gaussian_filter(wfs, sigma, kernel_half_width, derivative=True)
    smooth_wfs = gaussian_filter(wfs, sigma, kernel_half_width, derivative=False)

    return dict(wfs=smooth_wfs, dwfs=dwfs)
#

def find_rel_maxima(dwf, wf, thr):
    if thr <= 0:
        raise ValueError(f'The threshold must be positve, while here thr={thr}')
    #
    
    dwf_sign = np.sign(dwf)
    pos_to_neg = (dwf_sign[:-1] > 0) & (dwf_sign[1:] < 0)
    #
    
    transition_mask = np.zeros_like(dwf, dtype=bool)
    transition_mask[:-1] |= pos_to_neg
    transition_mask[1:]  |= pos_to_neg
    #
    
    # --- Regions where waveform is positive (above threshold)
    positive_mask = wf > thr

    # --- Overlap between positive regions and derivative transitions
    pile_mask = positive_mask & transition_mask
    
    wf_regions_labels, _ = label(pile_mask)
    n_regions = wf_regions_labels.max()  # number of distinct labeled regions
    return int(n_regions), wf_regions_labels
#