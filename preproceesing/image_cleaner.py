#!/usr/bin/env python
# coding: utf-8

# In[2]:


from PIL import Image, ImageFile
import math
from PIL import ImageFilter


def is_gray(a, b, c):
    r = 30
    if a + b + c < 350:
        return True
    if abs(a - b) > r:
        return False
    if abs(a - c) > r:
        return False
    if abs(b - c) > r:
        return False
    return True   


def cleanImage(img):
    # img = Image.open(image)
    im2 = img.convert("P", palette=Image.ADAPTIVE, colors=256)
    palette = im2.getpalette()
    for i in range(len(palette)//3):
        r = palette[3*i] 
        g = palette[3*i+1]
        b = palette[3*i+2]
        
        if is_gray(r, g, b):
            a = 1
            # palette[3*i:3*i+3] = [0,0,0]
        else:
            palette[3*i:3*i+3] = [255,255,255]
            
        gray = (palette[3*i] + palette[3*i+1] + palette[3*i+2])/3
        gray = math.floor(gray)
        palette[3*i:3*i+3] = [gray,gray,gray]
    
    im2.putpalette(palette)
    sharpened1 = im2.convert('RGB')
    sharpened1 = sharpened1.filter(ImageFilter.SHARPEN);
    # sharpened1 = sharpened1.filter(ImageFilter.MinFilter(size=3));
    # sharpened1.save(filename+"_out", optimize=True, format="JPEG")
    return sharpened1


# In[ ]:




