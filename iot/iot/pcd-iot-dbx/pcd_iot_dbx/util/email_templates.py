from jinja2 import Environment, BaseLoader

def render_template(template_str:str,data: dict):

    rtemplate = Environment(loader=BaseLoader(),autoescape=True).from_string(template_str)

    return rtemplate.render(**data)



ALERT = \
"""
<html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office"
    xmlns:w="urn:schemas-microsoft-com:office:word" xmlns:m="http://schemas.microsoft.com/office/2004/12/omml"
    xmlns="http://www.w3.org/TR/REC-html40">

<head>
    <meta name=Generator content="Microsoft Word 15 (filtered medium)"><!--[if !mso]><style>v\:* {behavior:url(#default#VML);}
o\:* {behavior:url(#default#VML);}
w\:* {behavior:url(#default#VML);}
.shape {behavior:url(#default#VML);}
</style><![endif]-->
    <style>
        <!--
        /* Font Definitions */
        @font-face {
            font-family: "Cambria Math";
            panose-1: 2 4 5 3 5 4 6 3 2 4;
        }

        @font-face {
            font-family: Calibri;
            panose-1: 2 15 5 2 2 2 4 3 2 4;
        }

        /* Style Definitions */
        p.MsoNormal,
        li.MsoNormal,
        div.MsoNormal {
            margin: 0in;
            font-size: 11.0pt;
            font-family: "Calibri", sans-serif;
        }

        h1 {
            mso-style-priority: 9;
            mso-style-link: "Heading 1 Char";
            mso-margin-top-alt: auto;
            margin-right: 0in;
            mso-margin-bottom-alt: auto;
            margin-left: 0in;
            font-size: 24.0pt;
            font-family: "Calibri", sans-serif;
            font-weight: bold;
        }

        span.Heading1Char {
            mso-style-name: "Heading 1 Char";
            mso-style-priority: 9;
            mso-style-link: "Heading 1";
            font-family: "Calibri Light", sans-serif;
            color: #2F5496;
        }

        span.EmailStyle23 {
            mso-style-type: personal-compose;
            font-family: "Times New Roman", serif;
        }

        .MsoChpDefault {
            mso-style-type: export-only;
            font-size: 10.0pt;
        }

        @page WordSection1 {
            size: 8.5in 11.0in;
            margin: 1.0in 1.0in 1.0in 1.0in;
        }

        div.WordSection1 {
            page: WordSection1;
        }
        -->
    </style><!--[if gte mso 9]><xml>
<o:shapedefaults v:ext="edit" spidmax="1026" />
</xml><![endif]--><!--[if gte mso 9]><xml>
<o:shapelayout v:ext="edit">
<o:idmap v:ext="edit" data="1" />
</o:shapelayout></xml><![endif]-->
</head>

<body bgcolor="#F3F4F4" lang=EN-US link=blue vlink=purple
    style='word-wrap:break-word;-webkit-text-size-adjust:none;text-size-adjust:none'>
    <div class=WordSection1>
        <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width="100%"
            style='width:100.0%;background:#F3F4F4'>
            <tr>
                <td style='padding:0in 0in 0in 0in'>
                    <div align=center>
                        <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width="100%"
                            style='width:100.0%'>
                            <tr>
                                <td style='padding:0in 0in 0in 0in'>
                                    <div align=center>
                                        <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width=650
                                            style='width:487.5pt;background:white'>
                                            <tr>
                                                <td width="100%" valign=top
                                                    style='width:100.0%;padding:3.75pt 0in 3.75pt 0in'>
                                                    <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0
                                                        width="100%" style='width:100.0%'>
                                                        <tr>
                                                            <td width="100%"
                                                                style='width:100.0%;padding:0in 0in 0in 0in'>
                                                                <p class=MsoNormal align=center
                                                                    style='text-align:center;mso-line-height-alt:7.5pt'>
                                                                    <img width=650 height=56
                                                                        style='width:6.7708in;height:.5833in'
                                                                        id="_x0000_i1026"
                                                                        src="https://cm-email-assets.s3.amazonaws.com/images/ContactMonkey-User-22srgIFKy5GSkzHagTtx1Te1cQQI5yrFu5UmQKDeowAA4kq9P12phtFg0oRyBPP4OobVnMtGpoAyo6yMZLg3F0aRTIBrsYAJbtfQEbCPC99lYnWzXrxNu1LzjxjDYeZm/Generic%20Branded%20Banner%20-%202021.png"
                                                                        alt="Nationwide logo">
                                                                    <o:p></o:p>
                                                                </p>
                                                            </td>
                                                        </tr>
                                                    </table>
                                                </td>
                                            </tr>
                                        </table>
                                    </div>
                                </td>
                            </tr>
                        </table>
                    </div>
                    <p class=MsoNormal><span style='display:none'>
                            <o:p>&nbsp;</o:p>
                        </span></p>
                    <div align=center>
                        <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width="100%"
                            style='width:100.0%'>
                            <tr>
                                <td style='padding:0in 0in 0in 0in'>
                                    <div align=center>
                                        <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width=650
                                            style='width:487.5pt;background:white'>
                                            <tr>
                                                <td width="100%" valign=top
                                                    style='width:100.0%;padding:0in 0in 0in 0in'>
                                                    <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0
                                                        width="100%" style='width:100.0%'>
                                                        <tr>
                                                            <td width="100%"
                                                                style='width:100.0%;padding:0in 0in 0in 0in'>
                                                                <p class=MsoNormal align=center
                                                                    style='text-align:center;mso-line-height-alt:7.5pt'>
                                                                    <img width=390 height=196
                                                                        style='width:4.0625in;height:2.0416in'
                                                                        id="_x0000_i1025"
                                                                        src="https://cm-email-assets.s3.amazonaws.com/images/ContactMonkey-User-f2rmoQQT5uS1O4uAy8d1ZXrIivw2OznhJajj47ugLBslbfBRBt44j3INKwLRkrB50OzXsXwbeclk0Wh6jFSYZNqD3DClFPPfdOBNWgd6cVgvdrfMxwg9LIOOcjxZcdBy/internet%20of%20things_1.png">
                                                                    <o:p></o:p>
                                                                </p>
                                                            </td>
                                                        </tr>
                                                    </table>
                                                    <p class=MsoNormal><span style='color:black;display:none'>
                                                            <o:p>&nbsp;</o:p>
                                                        </span></p>
                                                </td>
                                            </tr>
                                        </table>
                                    </div>
                                </td>
                            </tr>
                        </table>
                    </div>
                    <p class=MsoNormal><span style='display:none'>
                            <o:p>&nbsp;</o:p>
                        </span></p>
                    <div align=center>
                        <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width="100%"
                            style='width:100.0%'>
                            <tr>
                                <td style='padding:0in 0in 0in 0in'>
                                    <div align=center>
                                        <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width=650
                                            style='width:487.5pt;background:white'>
                                            <tr>
                                                <td width="100%" valign=top
                                                    style='width:100.0%;border:solid #141B4D 1.5pt;padding:3.75pt 0in 3.75pt 0in'>
                                                    <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0
                                                        width="100%" style='width:100.0%'>
                                                        <tr>
                                                            <td width="100%"
                                                                style='width:100.0%;padding:0in 15.0pt 7.5pt 15.0pt'>
                                                                <h1 style='margin:0in;line-height:120%'><strong><span
                                                                            style='font-size:16.0pt;line-height:120%;font-family:"Arial",sans-serif;color:#0047BB'>Exception table: {{ exception_table }}</span></strong><span
                                                                        style='font-size:16.0pt;line-height:120%;font-family:"Arial",sans-serif;color:#0047BB;font-weight:normal'>
                                                                        <o:p></o:p>
                                                                    </span></h1>
                                                            </td>
                                                        </tr>
                                                    </table>
                                                    <p class=MsoNormal><span style='color:black;display:none'>
                                                            <o:p>&nbsp;</o:p>
                                                        </span></p>
                                                    <p class=MsoNormal><span style='color:black;display:none'>
                                                            <o:p>&nbsp;</o:p>
                                                        </span></p>
                                                    <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0
                                                        width="100%" style='width:100.0%'>
                                                        <tr>
                                                            <td style='padding:0in 0in 0in 0in'>
                                                                <div align=center>
                                                                    <table class=MsoNormalTable border=1 cellspacing=0
                                                                        cellpadding=0 width="100%"
                                                                        style='width:100.0%;border-collapse:collapse;border:none'>
                                                                        <tr>
                                                                        {% for column in columns   %}
                                                                            <td
                                                                                style='border:solid black 1.0pt;background:white;padding:.75pt .75pt .75pt .75pt'>
                                                                                <p class=MsoNormal align=center
                                                                                    style='text-align:center'><span
                                                                                        style='font-size:9.0pt;font-family:"Arial",sans-serif;color:#222222'>{{ column }}
                                                                                        <o:p></o:p></span></p>
                                                                            </td>
                                                                        {% endfor %}
                                                                        </tr>
                                                                        
                                                                        {% for row in rows   %}
                                                                            <tr>
                                                                            {% for field in row %}
                                                                            <td
                                                                                style='border:solid black 1.0pt;border-left:none;background:white;padding:.75pt .75pt .75pt .75pt'>
                                                                                <p class=MsoNormal align=center
                                                                                    style='text-align:center'><span
                                                                                        style='font-size:9.0pt;font-family:"Arial",sans-serif;color:#222222'>{{ field }}
                                                                                        <o:p></o:p></span></p>
                                                                            </td>
                                                                            {% endfor %}
                                                                            </tr>
                                                                        {% endfor %}
                                                                    </table>
                                                                </div>
                                                            </td>
                                                        </tr>
                                                    </table>
                                                </td>
                                            </tr>
                                        </table>
                                    </div>
                                </td>
                            </tr>
                        </table>
                    </div>
                    <p class=MsoNormal><span style='display:none'>
                            <o:p>&nbsp;</o:p>
                        </span></p>
                    <div align=center>
                        <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width="100%"
                            style='width:100.0%'>
                            <tr>
                                <td style='padding:0in 0in 0in 0in'>
                                    <div align=center>
                                        <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0 width=650
                                            style='width:487.5pt;background:white'>
                                            <tr>
                                                <td width="100%" valign=top
                                                    style='width:100.0%;background:#0047BB;padding:3.75pt 0in 3.75pt 0in'>
                                                    <table class=MsoNormalTable border=0 cellspacing=0 cellpadding=0
                                                        width="100%" style='width:100.0%;word-break:break-word'>
                                                        <tr>
                                                            <td style='padding:7.5pt 15.0pt 7.5pt 15.0pt'>
                                                                <div>
                                                                    <div>
                                                                        <p style='margin:0in;line-height:15.75pt'><span
                                                                                style='font-size:10.5pt;font-family:"Arial",sans-serif;color:white'>For
                                                                                internal use only.</span><span
                                                                                style='font-size:10.5pt;font-family:"Arial",sans-serif;color:#555555'>
                                                                                <o:p></o:p>
                                                                            </span></p>
                                                                    </div>
                                                                </div>
                                                            </td>
                                                        </tr>
                                                    </table>
                                                </td>
                                            </tr>
                                        </table>
                                    </div>
                                </td>
                            </tr>
                        </table>
                    </div>
                </td>
            </tr>
        </table>
        <p class=MsoNormal style='content: &#34;cm_preview_injected&#34;'>
            <o:p>&nbsp;</o:p>
        </p>
    </div>
</body>

</html>
"""