import unittest
#import os
#import datetime
import logging

from quoteworker.tasks import get_annotations

logger = logging.getLogger(__name__)


class AnnotateTest(unittest.TestCase):

    def testAnnotate(self):
        results = get_annotations('1', 'This is some text with a quote, like "Help me" which was said by Rahul Bhargava')
        assert 'sentences' in results
        assert len(results['sentences']) == 1
        assert 'quotes' in results
        assert len(results['quotes']) == 1
        assert 'speaker' in results['quotes'][0]
        assert results['quotes'][0]['speaker'] == 'Rahul Bhargava'

    def testRealAnnotate(self):
        text = "Homeland Security chief on killed Kate Steinle: ‘Who?’\n\n  \n\n \n\n \n        \n        \t\t\t\t\n        \n        \n\n \n            \n\n \n\n     \n  \n\n\n                    \n\n   Text smaller   \n\n\n                    \n\n  Text bigger  \n\n\n                 \n             \n\n\n                     \n\n\n\n        \n\n \n\n\n Jeh Johnson, the secretary of the of Homeland Security, caused a firestorm on Capitol Hill when he was asked if federal authorities had contacted the family of the woman allegedly killed by an illegal immigrant – a story that’s shaken the nation and gone viral among the media – and he answered with little more than a shrug of the shoulders. \n\n\n\n\n Johnson professed no knowledge of the family. \n\n\n\n\n   Get the hottest, most important news stories on the Internet – delivered FREE to your inbox as soon as they break! Take just 30 seconds and sign up for WND’s Email News Alerts!   \n\n\n\n\n Specifically, he was asked during a congressional hearing if the White House or anyone in his department had contacted the family of Kate Steinle. \n\n\n\n\n “To who?” Johnson said. \n\n\n\n\n Rep. Steve Chabot replied: “To the family of the woman who was brutally murdered by this individual who had committed seven different felonies in four different states and my understanding had been deported, kept coming back, has the administration reached out to that family?” \n\n\n\n\n And Johnson’s answer: “I’m sorry, I don’t know the answer to that question, sir.” \n\n\n\n\n Conservative radio hosts Mark Levin and Michael Savage both expressed disbelief and outrage at Johnson’s response. \n\n\n\n\n Rush Limbaugh, meanwhile, said: “He doesn’t know who she is. The director of Homeland Security has no idea, when he hears the name Steinle family, he doesn’t know who it is.” \n\n\n\n\n And Bill O’Reilly, host of Fox News’ “The O’Reilly Factor,” said it was “hard to believe” Johnson didn’t recognize the Steinle name. \n\n\n        \n                                               \n        \n        \n\n \n            \n\n \n\n     \n  \n\n\n                    \n\n   Text smaller   \n\n\n                    \n\n  Text bigger  \n\n\n                 \n             \n\n\n                            \n\n \n                                     \n\n\n                     \n\n\n\n        \n                            \n                             \n\n\n        \n                     \n\n  "
        results = get_annotations('1', text)
        assert 'sentences' in results
        assert len(results['sentences']) == 15
        assert 'quotes' in results
        assert len(results['quotes']) == 7

    def testNoQuotes(self):
        results = get_annotations('1', 'This is some text with no quotes all all. Move along. Nothing to see here')
        assert 'sentences' in results
        assert len(results['sentences']) == 3
        assert 'quotes' in results
        assert len(results['quotes']) == 0


'''
    # this times out unfortunately :-(
    def testLongRunningAnnotate(self):
        with open(os.path.join('quoteworker', 'test', 'fixtures', '1340333046.txt'), 'r') as f:
            text = f.read()
            start = datetime.datetime.now()
            results = get_annotations('1340333046', text[0:95000])  # trim it to max corenlp length
            end = datetime.datetime.now()
            duration = end - start
            logging.info("Took {} seconds total".format(duration.seconds))
            assert 'sentences' in results
            assert 'quotes' in results
'''
