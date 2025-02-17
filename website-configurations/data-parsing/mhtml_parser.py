from bs4 import BeautifulSoup

results = {
    'status': None,
    'jbkFormPresent': False
}

def check_for_broken_page(soup):
    # 1) cloudflare challenge
    cloudflare_error = soup.find(id='challenge-error-text')
    if cloudflare_error:
        results['status'] = 'CLOUDFLARE_CHALLENGE'
        return True
    
    # 2) 404 error
    page_title = soup.title.text if soup.title else 'No title found'
    if '403' in page_title or '404' in page_title:
        results['status'] = 'ERROR_403_404'
        return True

    return None

def mhtml_parser(file):
    soup = BeautifulSoup(file, features="html.parser")
    page_error = check_for_broken_page(soup)
    if page_error == None:
        results['status'] = 'HTMLDownloadSuccess'
        inputs = soup.find_all('input')
        forms = soup.find_all('form')

        for item in (inputs+forms):
            # confirm form was successfully found in the HTML
            if item.get('id') and 'jbk' in item.get('id'):
                results['jbkFormPresent'] = True
                break

    return results
