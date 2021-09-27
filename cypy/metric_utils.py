import numpy as np
from easydict import EasyDict
from sklearn.metrics import roc_curve, auc, confusion_matrix
from scipy.optimize import brentq
from scipy.interpolate import interp1d


def find_best_threshold(y_trues, y_preds):
    print("Finding best threshold...")
    best_thre = 0.5
    best_metrics = None
    candidate_thres = list(np.unique(np.sort(y_preds)))
    for thre in candidate_thres:
        metrics = cal_metrics(y_trues, y_preds, threshold=thre)
        if best_metrics is None:
            best_metrics = metrics
            best_thre = thre
        elif metrics.ACER < best_metrics.ACER:
            best_metrics = metrics
            best_thre = thre
    print(f"Best threshold is {best_thre}")
    return best_thre, best_metrics


def cal_auc(y_trues, y_preds):
    fpr, tpr, thresholds = roc_curve(y_trues, y_preds)
    AUC = auc(fpr, tpr)
    return AUC
    

def cal_metrics(y_trues, y_preds, threshold=0.5):
    metrics = EasyDict()
    
    fpr, tpr, thresholds = roc_curve(y_trues, y_preds)
    metrics.AUC = auc(fpr, tpr)
    
    metrics.EER = brentq(lambda x: 1. - x - interp1d(fpr, tpr)(x), 0., 1.)
    metrics.Thre = float(interp1d(fpr, thresholds)(metrics.EER))

    if threshold == 'best':
        _, best_metrics = find_best_threshold(y_trues, y_preds)
        return best_metrics
    elif threshold == 'auto':
        threshold = metrics.Thre

    prediction = (np.array(y_preds) > threshold).astype(int)
    
    TN, FP, FN, TP = confusion_matrix(y_trues, prediction).ravel()
    
    metrics.ACC = (TP + TN) / len(y_trues)
    metrics.TPR = float(TP / (TP + FN))
    metrics.TNR = float(TN / (TN + FP))
    metrics.APCER = float(FP / (TN + FP))
    metrics.BPCER = float(FN / (FN + TP))
    metrics.ACER = (metrics.APCER + metrics.BPCER) / 2
    
    return metrics