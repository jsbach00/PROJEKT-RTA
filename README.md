# PROJEKT-RTA
generalnie wszystko jak u Maćka w instrukcji tylko topic będzie nie transactions, a pollution.

plik db_preview.ipynb pozwala podejrzeć jak zapisały się dane do baz.

na każde z 5 zanieczyszczeń tworzona jest oddzielna tabela sqlowa

z uwag, wydaje mi się że godzina jako 5 sek to trochę za szybko, strasznie to wzystko zapierdziela ale to można ustawić już w kodzie producenta jak się chce

dane, czyli excele z 2023 muszą być w folderze data w postaci /home/jovyan/notebooks/data na tym środowisku jak już się wam odpali

1. Pobrać pliki i rozpakować zipa
2. W terminalu wpisać (przy tym musi być otwarta apka docker, całość się trochę czasu pobiera): 
cd ścieżka do zipa
docker compose up
3. http://localhost:8888/lab
4. Odpalić terminal i wpisać:
docker exec -it jupyter /bin/bash
/home/jovyan/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server broker:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic pollution
5. git clone https://github.com/jsbach00/PROJEKT-RTA
(aby ułatwić życie, od razu mamy folder data i kody py)
 
7. Odpalam 2 nowe terminale:
W obydwu wchodzę w katalog w którym jest plik producenta i konsumenta
Odpalam w pierwszym python producent_zanieczyszczen.py, potem w drugim python konsument_zanieczyszczeń.py
