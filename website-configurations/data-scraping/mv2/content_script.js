function injectForm() {
    setTimeout(() => {
        console.log('injecting form');

        // create new div with form element
        var newDiv = document.createElement('div');       
        
        newDiv.innerHTML = `
            <form id="jbk392-personal-data-form" class="form__container">
                <div>
                    <label for="firstName"> First Name </label>
                    <input type="text" id="jbk392-firstName" name="firstName">
                </div>
                <div>
                    <label for="lastName"> Last Name </label>
                    <input type="text" id="jbk392-lastName">
                </div>
                <div>
                    <label for="address"> Home Address </label>
                    <input type="text" id="jbk392-address"></div>
                <div>
                    <label for="city"> City </label>
                    <input type="text" id="jbk392-city"></div>
                <div>
                    <label for="state"> State </label>
                    <input type="text" id="jbk392-state">
                </div>
                    <div><label for="zipcode"> Zipcode </label>
                    <input type="text" id="jbk392-zipcode">
                </div>
                <div>
                    <label for="country"> Country </label>
                    <input type="text" id="jbk392-country">
                </div>
                <div>
                    <label for="phone"> Phone: </label>
                    <input type="tel" id="jbk392-phone">
                </div>
                <div><label for="email"> Email </label>
                    <input type="email" id="jbk392-email">
                </div>
                <button id="jbk392-submit__buton"> Submit </button>
            </form>
        `;

        // find a node to attach to
        for(let el of document.body.querySelectorAll('*')) {
            const parent = el.parentNode;
            const isBlacklistedId = checkIdName(el.id, parent.id);
            if ( (el.tagName == 'DIV' || el.tagName == 'SPAN') && !isBlacklistedId ) {
                el.setAttribute('class', '');
                el.style.display = 'flex';

                el.appendChild(newDiv);

                // check to make sure it really attached
                if (document.getElementById('jbk392-personal-data-form')) {
                    break;
                }
            }
        }  
        
        try {
            // try to trigger a form_start event, by filling out the form
            document.getElementById('jbk392-firstName').focus();
            document.getElementById('jbk392-firstName').value = 'Louisa';
            document.getElementById('jbk392-firstName').blur();
        
            document.getElementById('jbk392-lastName').focus();
            document.getElementById('jbk392-lastName').value = 'Ikeman';
            document.getElementById('jbk392-lastName').blur();
        
            document.getElementById('jbk392-address').focus();
            document.getElementById('jbk392-address').value = '370 Jay Street';
            document.getElementById('jbk392-address').blur();
        
            document.getElementById('jbk392-city').focus();
            document.getElementById('jbk392-city').value = 'Brooklyn';
            document.getElementById('jbk392-city').blur();
        
            document.getElementById('jbk392-state').focus();
            document.getElementById('jbk392-state').value = 'New York';
            document.getElementById('jbk392-state').blur();
        
            document.getElementById('jbk392-zipcode').focus();
            document.getElementById('jbk392-zipcode').value = '11201';
            document.getElementById('jbk392-zipcode').blur();
        
            document.getElementById('jbk392-country').focus();
            document.getElementById('jbk392-country').value = 'United States';
            document.getElementById('jbk392-country').blur();
        
            document.getElementById('jbk392-phone').focus();
            document.getElementById('jbk392-phone').value = '1231231234';
            document.getElementById('jbk392-phone').blur();
        
            document.getElementById('jbk392-email').focus();
            document.getElementById('jbk392-email').value = 'apple@banana.com';
            document.getElementById('jbk392-email').blur();

            pageData = document.documentElement.outerHTML;
        } catch (e) { 
            chrome.runtime.sendMessage({action: 'failed-to-inject-form', data: {'html': document.documentElement.outerHTML, 'error': e} })
        }
        
        const submitButton = document.getElementById('jbk392-submit__buton');
        if (submitButton !== null) {
            // Dispatch the event on the form
            submitButton.addEventListener("click", (event) => {
                chrome.runtime.sendMessage({action: 'form-submitted', data: pageData});
            });
            submitButton.click();
        } else {
            chrome.runtime.sendMessage({action: 'failed-to-inject-form', data: pageData })
        }
        
    }, "10000"); 
}

function checkIdName(el, parentEl) {
    // avoid attaching to a cookie dialog (ex: hediyemen.com)
    if (el.includes('cookie') || parentEl.includes('cookie')) {
        return true;
    }
    return false;
}

chrome.runtime.onMessage.addListener(
    function(request, sender, sendResponse) {
      if (request.action === 'page-loaded') {
        const timestamp = new Date().toISOString()
        const dataItem = {
            url: 'content-script',
            filename: 'page-loaded-confirmation',
            requestType: 'logs',
            metadata: timestamp,
            status: 'success'
        }
        chrome.runtime.sendMessage({action: 'logItem', data: dataItem})
        injectForm();
      }
    }
);