# !/usr/bin/python
# -*- coding:utf-8 -*-

import sys
import jieba
import jieba.posseg
import codecs


if __name__ == "__main__":
    f = codecs.open('24.novel.txt')
    str = f.read()
    f.close()

    seg = jieba.posseg.cut(str)
    for s in seg:
        print (s.word, s.flag, '|',)
        # print s.word, '|',
