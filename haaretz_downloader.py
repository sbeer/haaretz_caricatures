__author__ = 'sbeer'

import urllib
import os
import httplib
import time, logging, httplib, socket
import simplejson
import urllib2
import re
import socket
import pandas as pd
import json
import gspread
from oauth2client.client import SignedJwtAssertionCredentials
import csv
import numpy as np

import sys
sys.stdout.flush()

folder = r'/home/ec2-user/work/haaretz_caricatures'
links_filename = 'link_list.json'
url_name = "http://www.haaretz.co.il/opinions/caricatures"
socket_default_timeout = 30
authors = ['http://www.haaretz.co.il/misc/writers/1.681419?listId=7.1283159&page={0}#listAnchor7.1283159',
           'http://www.haaretz.co.il/misc/writers/1.849?listId=7.1283159&page={0}#listAnchor7.1283159']
page_offset_range = (0,10)
download_start_offset = 0
columns=['Start Date','End Date','Headline','Text','Media','Media Credit','Media Caption','Media Thumbnail','Type','Tag']
excel_filename = 'table1.csv'
credential_filename = 'haaretzcaricatures-54bd19a26703.json'
scope_url = "https://spreadsheets.google.com/feeds"
spreadsheet_name = "haaretz_caricatures"
upload_full_speadsheet = False

def main():
    print "This will launch the haaretz caricature downloader\n"
    # import ipdb; ipdb.set_trace()
    stored_page_links = load_stored_page_links(links_filename)
    socket.setdefaulttimeout(socket_default_timeout)
    page_links = get_links_to_caricatures(authors,url_name,page_offset_range)
    new_links = check_for_new_caricature_links(page_links,stored_page_links)
    socket.setdefaulttimeout(socket_default_timeout-10)

    page_links_lst = list(enumerate(list(new_links)))
    if(len(new_links)>0):
        print "updating spreadsheet\n"
        table_of_content = download_caricatures_form_links(page_links_lst[download_start_offset:])
        toc_df = pd.DataFrame(table_of_content)
        toc_df = toc_df[columns]

        if(os.path.exists(os.path.join(folder,excel_filename))):
            print 'Loading previous stored excel\n'
            with open(os.path.join(folder,excel_filename), 'a') as f:
                toc_df.to_csv(f, header=False, index=False)
            f.close()
        else:
            toc_df.to_csv(os.path.join(folder,excel_filename), index=False)
    else:
        print "No caricature to update in spreadsheet\n"


    print "Updating to remote spreadsheet\n"
    local_spreadsheet = pd.read_csv(os.path.join(folder,excel_filename)).replace(np.nan,' ', regex=True)
    local_media_records = list(local_spreadsheet.Media)
    print "total local media records\n"

    remote_ws = get_remote_worksheet(scope_url, spreadsheet_name)
    remote_records = remote_ws.get_all_records(head=1)
    remote_media_records = [r.get('Media') for r in remote_records if r.get('Media') is not '']

    to_update_records = [lr for lr in list(local_spreadsheet.to_records(index=False)) if lr[4] not in remote_media_records]
    update_records_to_remote_worksheet(get_remote_worksheet(scope_url, spreadsheet_name), to_update_records, upload_full_speadsheet)

    print 20*""
    print "update post in: https://docs.google.com/spreadsheets/d/1yY31Yt5V-VMCJWQXa1k5tCLwlo2oWa-KXj0NkwjyHG4/edit#gid=0"
    print "website url: http://tx.technion.ac.il/~sbeer/haaretz_caricatures/"
    print "exiting haaretz caricature app\n"
    print 20*""


def get_remote_worksheet(scope_url, spreadsheet_name):
    json_key = json.load(open(os.path.join(folder,credential_filename)))
    scope = [scope_url]
    credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)
    gc = gspread.authorize(credentials)
    wks = gc.open(spreadsheet_name)
    return wks.sheet1


def update_records_to_remote_worksheet(rws, to_update_records, upload_full_speadsheet):
    if upload_full_speadsheet:
        f = open(os.path.join(folder,excel_filename), 'rb')
        excel_spreadsheet = csv.reader(f)
        for i,record in enumerate(excel_spreadsheet):
            if(i==0): continue #skip column names
            rws.insert_row(tuple(record),i+2)
        print "finished updating spreadsheet with {0} new entries".format(i)
        f.close()
    else:
        for record in to_update_records:
            rws.append_row(record)
        print "finished updating spreadsheet with {0} new entries".format(len(to_update_records))



def download_caricatures_form_links(links):
    table_of_content = []
    for i,cp in links:

        r=0
        try:
            car_page_to_open = urllib2.urlopen("http://www.haaretz.co.il"+cp, timeout=15)
            caricature_page = car_page_to_open.read()
        except (urllib2.URLError, httplib.IncompleteRead, urllib2.URLError, socket.timeout), e :
            r=r+1
            print "Re-trying, attempt -- ",r
            time.sleep(2)
            pass


        car_page_to_open.close()

        picture_links = ["http://www.haaretz.co.il"+re.findall('.*?source srcset="(.*?)"', caricature_page)[0]]
        pic_date = re.findall('.*?itemprop="datePublished">(.*?) ', caricature_page)
        if len(pic_date)>0:
            pd_data = pic_date[0].split('.')
            pd = '.'.join([pd_data[1],pd_data[0],pd_data[2]])
        else:
            pd = 'non_date'
        pic_print_date = '_'.join(pd.split('.'))


        for pl in picture_links:
            print '.',
            head, tail = os.path.splitext(pl)
            dest_file = os.path.join(folder,'images',pic_print_date+tail)

            count = 0
            while os.path.exists(dest_file):
                count += 1
                dest_file = os.path.join(folder,'images', '%s-%d%s' % (pic_print_date, count, tail))

            #dowload the file
#         try:
#             urllib.urlretrieve(pl,dest_file)
#         except (urllib2.URLError, urllib2.URLError, socket.timeout), e :
#             r=r+1
#             print "Re-trying Retrive , attempt -- ",r
#             time.sleep(2)
#             pass

            ff, fname = os.path.split(dest_file)

            table_of_content.append({'End Date': '/'.join(pd.split('.')),
             'Headline': '<h1 style="color:black;text-align:right;font-size:35px">please add headline</h1>',
             'Media': pl,
             'Media Caption': '',
             'Media Credit': 'caricatures from Haaretz newspaper',
             'Media Thumbnail': '',
             'Start Date': '/'.join(pd.split('.')),
             'Tag': '',
             'Text': '<p style="text-align:right;font-size:20px">add text</p>',
             'Type': ''})
    return table_of_content


def check_for_new_caricature_links(page_links, stored_page_links ):
    new_links = page_links - set(stored_page_links).intersection(page_links)
    print 'link file contains ', len(stored_page_links), 'links '
    print 'found ', len(new_links), 'new links for caricatures'
    stored_page_links = stored_page_links + list(new_links)

    import simplejson
    f = open(os.path.join(folder,links_filename), 'w')
    simplejson.dump(list(stored_page_links), f)
    f.close()
    return new_links



def get_links_to_caricatures(authors_links, url_name, page_offset_range):
    
	offsets = range(page_offset_range[0],page_offset_range[1])
	page_links = set()
	for auth in authors_links:
		for offset in offsets:
			print '.',
			r=0
			try:
				page_to_open = urllib2.urlopen(auth.format(offset))
				target_page = page_to_open.read()
			except (urllib2.URLError, httplib.IncompleteRead, urllib2.URLError, socket.timeout), e :
				r=r+1
				print "Re-trying, attempt -- ",r
				time.sleep(2)
				pass

			all_links = re.findall('.*?href="(.*?)"', target_page)
			caricature_links = set([page for page in all_links if '/opinions/caricatures/' in page and not '#' in page])
			caricature_pages_not_premium = set([page for page in caricature_links if not 'premium' in page])
			page_links = page_links | caricature_pages_not_premium

	print 'Total # of caricature links: ', len(page_links)
	return page_links


def run_with_retries(func, num_retries, sleep = None, exception_types = Exception, on_retry = None):
    for i in range(num_retries):
        try:
            return func()  # call the function
        except exception_types, e:
            # failed on the known exception
            if i == num_retries - 1:
                raise  # this was the last attempt. reraise
            print 'operation failed {0} with error {1}. will retry {2} more times'.format(func, e, num_retries - i - 1)
            if on_retry is not None:
                on_retry
            if sleep is not None:
                time.sleep(sleep)
    assert 0  # should not reach this point

def load_stored_page_links(links_filename):
    # Load stored links
    if(os.path.exists(os.path.join(folder,links_filename))):
        print 'Loading previous stored links'
        with open(os.path.join(folder,links_filename)) as f:
          return simplejson.load(f)
        f.close()
    else:
        return []






if __name__ == "__main__":
    main()
