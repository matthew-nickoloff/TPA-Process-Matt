(function(global){
    var TABCONTAINER = '.sideBarCard';

    var controller = {
        init: function(){
            this.addListener();
            this.disableAutoComplete();
        },
        addListener: function(){
            var self = this;
            document.querySelector(TABCONTAINER).addEventListener('click', function(_){
                self.disableAutoComplete();
                self.tab1ModalControl();
            });
        },
        disableAutoComplete: function(){
            var inputs = inputs = document.getElementsByTagName('input');
            Array.from(inputs).forEach(function(v){
                v.setAttribute('autocomplete', 'off');
            });
        },
        tab1ModalControl: function(){
            setTimeout(function(){
                document.getElementById('tab1_modal_close').addEventListener('click', function(){
                    document.getElementById('tab1_modal').className = "modal";
                });
                document.getElementById('tab1_modal_open').addEventListener('click', function(){
                    document.getElementById('tab1_modal').className = "modal active";
                });
            }, 1000);
            
        }
    }
    
    global.setTimeout(function(){
        controller.init();
    }, 800);


})(window);

