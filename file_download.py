import requests

url = 'https://d2xzmw6cctk25h.cloudfront.net/geekbrains/public/ckeditor_assets/pictures/10244/retina-1be15433f9ccb024de8e4c25ab3f9f87.png'

response = requests.get(url)
with open('tmp.png', 'wb') as file:
    file.write(response.content)
