"""Fail-closed sampled process-tree RSS accounting for benchmark evidence.

Sampling is observational, misses between-sample spikes, and may count shared
mappings twice. A permission/enumeration/sampling error invalidates the *whole*
trial's peak; a stale partial observation never passes a required memory cap.
"""
from __future__ import annotations
import threading


class RSSSampler:
    def __init__(self, *, interval=.01, psutil_module=None):
        self.stop=threading.Event()
        self.interval=interval
        self.samples=0
        self.attempts=0
        self.exited_process_races=0
        self.errors=[]
        self.error_count=0
        self.thread=None
        self._observed_peak=None
        self._first_sample=None
        self._last_sample=None
        self._peak_sample=None
        self._psutil=psutil_module
        self._process=None

    @property
    def peak(self):
        return None if self.errors else self._observed_peak

    def _error(self,scope,exc):
        self.error_count+=1
        if len(self.errors)<8:
            self.errors.append(scope+':'+type(exc).__name__)

    def sample(self):
        self.attempts+=1
        try:
            descendants=self._process.children(recursive=True)
        except Exception as exc:
            self._error('enumeration',exc)
            return
        total=0;parent_rss=0;observed_children=0;complete=True
        for index,process in enumerate([self._process,*descendants]):
            try:
                value=process.memory_info().rss
                if type(value) is not int or value<0:
                    raise ValueError('invalid RSS metric')
                total+=value
                if index==0:
                    parent_rss=value
                else:
                    observed_children+=1
            except self._psutil.NoSuchProcess as exc:
                if index==0:
                    complete=False;self._error('own_process_missing',exc)
                else:
                    self.exited_process_races+=1
            except Exception as exc:
                complete=False;self._error('process_rss',exc)
        if complete:
            self.samples+=1
            attribution={'tree_rss_bytes':total,'parent_rss_bytes':parent_rss,
                'descendant_rss_bytes':total-parent_rss,
                'observed_descendant_count':observed_children}
            if self._first_sample is None:
                self._first_sample=attribution
            self._last_sample=attribution
            if self._observed_peak is None or total>self._observed_peak:
                self._peak_sample=attribution
            self._observed_peak=max(self._observed_peak or 0,total)

    def __enter__(self):
        try:
            if self._psutil is None:
                import psutil
                self._psutil=psutil
            self._process=self._psutil.Process()
        except Exception as exc:
            self._error('initialization',exc)
            return self
        try:
            self.sample()
        except Exception as exc:
            self._error("initial_sample",exc)
        def loop():
            while not self.stop.wait(self.interval):
                try:
                    self.sample()
                except Exception as exc:
                    self._error('unexpected_sampler',exc)
                    return
        self.thread=threading.Thread(target=loop,name='benchmark-rss',daemon=True)
        try:
            self.thread.start()
        except Exception as exc:
            self._error("thread_start",exc)
            self.thread=None
        return self

    def __exit__(self,*args):
        self.stop.set()
        if self.thread is not None:
            try:
                self.thread.join()
            except Exception as exc:
                self._error("thread_join",exc)
        if self._process is not None:
            try:
                self.sample()
            except Exception as exc:
                self._error('final_sample',exc)

    def report(self):
        return {'complete':not self.errors and self.samples>0,
            'samples':self.samples,'attempts':self.attempts,
            'exited_process_races':self.exited_process_races,'error_count':self.error_count,'errors':list(self.errors),
            'peak_rss_bytes':self.peak,
            'first_complete_sample':dict(self._first_sample) if self._first_sample else None,
            'last_complete_sample':dict(self._last_sample) if self._last_sample else None,
            'peak_complete_sample':dict(self._peak_sample) if self._peak_sample else None,
            'attribution_scope':'Parent and descendant current RSS from the same non-atomic sampling sweep; diagnostic snapshots remain available after collection errors but do not make an incomplete trial eligible.',
            'observational_limits':'10ms samples may miss spikes and double-count shared mappings; not a hard memory cap'}
