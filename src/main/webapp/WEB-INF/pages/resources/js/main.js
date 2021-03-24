$( document ).ready(function() {
    $('.rateYo').each(function () {
        $(this).rateYo({
            rating: this.dataset.rating,
            starWidth: "20px",
            numStars: 5,
            readOnly: true
        });
    });
    feather.replace();
    $("#sidebar").click(function(){
        $(".sidebar").toggleClass("toggled").one("transitionend",function(){
            setTimeout(function(){window.dispatchEvent(new Event("resize"))},100)
        });
        $("#black_sheet").toggle();
    });
    $(".dropdown").click(function(){
        var menu = $(this).find('.dropdown-menu')[0];
        var wasOpened = $(menu).css("display") === 'block';
        $(".dropdown-menu").slideUp("fast");
        if (!wasOpened){
            $(this).find('.dropdown-menu').slideToggle("fast");
        }
    });
    // fillCounters();
});

$(document).on("click", function(event){
    var $trigger = $(".dropdown");
    if($trigger !== event.target && !$trigger.has(event.target).length){
        $(".dropdown-menu").slideUp("fast");
    }
    if ($( window ).width() > 991.98){
        return;
    }
    $trigger = $(".sidebar");
    var $trigger2 = $("#sidebar");
    if($trigger !== event.target && !$trigger.has(event.target).length
        && event.target.id !== "sidebar" && !$trigger2.has(event.target).length){
        $(".sidebar").removeClass("toggled").one("transitionend",function(){
            setTimeout(function(){window.dispatchEvent(new Event("resize"))},100)
        });
        $("#black_sheet").hide();
    }
});
function confirmDelete(){
    return confirm("Вы подтверждаете удаление?");}
function confirmComplete(){
    return confirm("Вы подтверждаете что задача выполнена? Произоейдет ее удаление");}
function confirmChange(){
    return confirm("Вы подтверждаете Изменение?");
}
function fileInput(fi_container_class, fi_button_class, fi_filename_class, fi_button_text) {
    if (document.getElementById("divFile")){
        return;
    }
    fi_container_class	=	fi_container_class	||	'fileUpload';
    fi_button_class		=	fi_button_class		||	'fileBtn';
    fi_filename_class	=	fi_filename_class	||	'fileName';
    fi_button_text		=	fi_button_text		||	'Обзор...';
    var fi_file = $('input[type=file]');
    fi_file.css('display', 'none');
    var fi_str = '<div id="divFile" class="'+fi_container_class+'"><div class="'+fi_button_class+'">'+fi_button_text+'</div><div class="'+fi_filename_class+'">Выберите файл</div></div>';
    fi_file.after(fi_str);
    var fi_count = fi_file.length;
    for (var i = 1; i <= fi_count; i++) {
        var fi_file_name = fi_file.eq(i-1).attr('name');
        $('.'+fi_container_class).eq(i-1).attr('data-name', fi_file_name);
    }
    $('.'+fi_button_class).on('click', function() {
        var fi_active_input = $(this).parent().data('name');
        $('input[name='+fi_active_input+']').trigger('click');
    });
    fi_file.on('change', function() {
        var fi_file_name = $(this).val();
        var fi_real_name = $(this).attr('name');
        var fi_array = fi_file_name.split('\\');
        var fi_last_row = fi_array.length - 1;
        fi_file_name = fi_array[fi_last_row];
        $('.'+fi_container_class).each(function() {
            var fi_fake_name = $(this).data('name');
            if(fi_real_name == fi_fake_name) {
                $('.'+fi_container_class+'[data-name='+fi_real_name+'] .'+fi_filename_class).html(fi_file_name);
            }
        });
    });
}

$(function() {
    $('.scrollup').click(function() {
        $("html, body").animate({
            scrollTop:0
        },1000);
    })
});

$(window).scroll(function() {
    if ($(this).scrollTop()>200) {
        $('.scrollup').fadeIn();
    }
    else {
        $('.scrollup').fadeOut();
    }
});
function fillCounters() {
    document.getElementById("counter1").innerHTML = '<a href="//www.liveinternet.ru/click" '+
        'target="_blank"><img src="//counter.yadro.ru/hit?t45.13;r'+
        escape(document.referrer)+((typeof(screen)=='undefined')?'':
            ';s'+screen.width+'*'+screen.height+'*'+(screen.colorDepth?
            screen.colorDepth:screen.pixelDepth))+';u'+escape(document.URL)+
        ';h'+escape(document.title.substring(0,150))+';'+Math.random()+
        '" alt="" title="LiveInternet" '+
        'border="0" width="31" height="31"/></a>';
}
var acc = document.getElementsByClassName("accordions");
var i;

for (i = 0; i < acc.length; i++) {
    acc[i].addEventListener("click", function() {
        this.classList.toggle("active");
        var panel = this.nextElementSibling;
        if (panel.style.maxHeight){
            panel.style.maxHeight = null;
        } else {
            panel.style.maxHeight = panel.scrollHeight + "px";
        }
    });
}