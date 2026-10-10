"""Deterministic image-core lifetime checks, including the error path."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from benchmarks.real_world import media

class ImageLifetimeTests(unittest.TestCase):
    def exercise(self, fail_thumbnail):
        try:
            from PIL import Image,ImageOps
        except ImportError:
            self.skipTest('Pillow required')
        with tempfile.TemporaryDirectory() as temp:
            task=media.prepare('images',Path(temp),1,17)[0]
            originals=(ImageOps.exif_transpose,Image.Image.convert,Image.Image.thumbnail,Image.Image.close)
            tracked={};closed=set()
            def transpose(source,*args,**kwargs):
                tracked['source']=source
                oriented=originals[0](source,*args,**kwargs)
                self.assertIsNot(oriented,source)
                tracked['oriented']=oriented
                return oriented
            def convert(image,*args,**kwargs):
                if image is tracked.get('oriented'):
                    self.assertIn(id(tracked['source']),closed)
                    transformed=originals[1](image,*args,**kwargs)
                    self.assertIsNot(transformed,image)
                    tracked['transformed']=transformed
                    return transformed
                return originals[1](image,*args,**kwargs)
            def thumbnail(image,*args,**kwargs):
                if image is tracked.get('transformed'):
                    self.assertIn(id(tracked['oriented']),closed)
                    if fail_thumbnail:raise RuntimeError('planned thumbnail failure')
                return originals[2](image,*args,**kwargs)
            def close(image):
                closed.add(id(image))
                return originals[3](image)
            with patch.object(ImageOps,'exif_transpose',side_effect=transpose), \
                 patch.object(Image.Image,'convert',convert), \
                 patch.object(Image.Image,'thumbnail',thumbnail), \
                 patch.object(Image.Image,'close',close):
                if fail_thumbnail:
                    with self.assertRaisesRegex(RuntimeError,'planned thumbnail failure'):
                        media.run_task(task)
                else:
                    result=media.run_task(task)
                    self.assertEqual(result['size'],task['expected_size'])
            self.assertEqual(set(tracked),{'source','oriented','transformed'})
            self.assertTrue(all(id(value) in closed for value in tracked.values()))

    def test_success_releases_each_independent_full_size_core(self):
        self.exercise(False)

    def test_failure_releases_all_opened_cores(self):
        self.exercise(True)
