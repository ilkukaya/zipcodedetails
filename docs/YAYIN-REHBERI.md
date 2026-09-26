# ZIPCodeDetails — Yayın ve Gelir Rehberi (Türkçe)

Bu rehber teknik bilgi gerektirmeden siteyi büyütmek ve gelir elde etmek için yapmanız
gerekenleri sırasıyla anlatır. Kod tarafında **her şey hazır**: aşağıdaki kimlikleri
(ID) aldığınızda tek yapmanız gereken bunları GitHub'a "Variable" olarak eklemek.
Site otomatik olarak yeniden yayınlanır.

---

## Ayarları nereye gireceğim? (tek yer)

Site artık **Netlify'ın kendi sunucularında** derleniyor. Tüm ayarlar Netlify'da:

1. https://app.netlify.com/projects/zipcodedetails → **Project configuration → Environment variables**
2. **Add a variable** → Anahtar (ör. `ADSENSE_CLIENT`) ve Değer (ör. `ca-pub-123…`) → Kaydet
3. **Deploys** sekmesi → **Trigger deploy → Deploy project** (≈10 dk sonra canlıda)

> Değişken boş bırakılırsa o özellik sitede görünmez; hiçbir şey bozulmaz.
> Bu adımları Claude'dan da isteyebilirsiniz: kimliği (ID) mesajla gönderin, o eklesin.

### Tek seferlik: GitHub'ı Netlify'a bağlayın (otomatik yayın için)
Netlify → **Project configuration → Build & deploy → Continuous deployment → Link repository**
→ GitHub → `ilkukaya/zipcodedetails` → dal: `main`. Bundan sonra `main`'e yapılan her
değişiklik otomatik yayınlanır. (Derleme ayarları `netlify.toml` dosyasından gelir, bir
şey değiştirmenize gerek yok.)

## 1. Hafta: Temeller (hepsi ücretsiz)

### ✅ Google Search Console (en önemli adım)
1. https://search.google.com/search-console → **Mülk ekle → URL öneki** → site adresiniz
2. Doğrulama yöntemi **HTML etiketi** → `content="…"` içindeki kodu kopyalayın
3. Netlify değişkeni: `GOOGLE_SITE_VERIFICATION` = kopyaladığınız kod → yeniden deploy
4. Doğrulayın → **Site haritaları** → `sitemap-index.xml` gönderin

### ✅ Bing Webmaster Tools (ChatGPT/Copilot aramaları da Bing kullanır)
1. https://www.bing.com/webmasters → **Search Console'dan içe aktar** (en kolayı)
   veya meta etiket → Variable: `BING_SITE_VERIFICATION`
2. Sitemap gönderin. IndexNow zaten her deploy'da otomatik bildiriyor.

### ✅ Analitik (birini seçin)
- **Google Analytics 4** (ücretsiz): Mülk oluştur → "G-XXXX" ölçüm kimliği → Netlify değişkeni `GA4_ID`
- **Cloudflare Web Analytics** (ücretsiz, çerezsiz): Token → Netlify değişkeni `CF_ANALYTICS_TOKEN`

### ✅ İletişim formu
Netlify panelinde **Forms** özelliğini açın (bizim tarafımızdan açıldı; açık değilse
Project configuration → Forms → Enable). Gelen mesajlar Netlify → Forms'ta görünür.
İsterseniz Netlify değişkeni `CONTACT_EMAIL` ile sayfada e-posta da gösterilir.

---

## 2. Alan adı (önerilir, ~10 $/yıl — tek ücretli kalem)

`zipcodedetails.netlify.app` çalışır ama AdSense onayı ve marka güveni için kendi alan
adınız çok daha iyidir.

1. Alan adını alın (Cloudflare Registrar / Namecheap / Porkbun — maliyet fiyatına)
2. Netlify → Project → **Domain management → Add a domain** → talimatları izleyin
   (DNS kayıtları; HTTPS sertifikası otomatik ve ücretsiz)
3. Netlify değişkeni: `SITE_URL` = `https://sizin-alanadiniz.com` → yeniden deploy
   (tüm canonical, sitemap, schema adresleri otomatik güncellenir)
4. Search Console'a yeni alan adını ekleyin ve sitemap'i tekrar gönderin

---

## 3. Reklam geliri

### Google AdSense
1. https://adsense.google.com → site adresinizi ekleyin
2. Size verilen yayıncı kimliği `ca-pub-XXXXXXXXXXXXXXXX` → Netlify değişkeni `ADSENSE_CLIENT`
   → yeniden deploy. Bu, doğrulama kodunu ve **ads.txt** dosyasını otomatik ekler.
3. AdSense'te "Siteyi incelemeye gönder". Onay genelde 1–4 hafta sürer.
4. **Onaylandıktan sonra:** AdSense → Reklamlar → **Reklam birimine göre** → 3 adet
   "Görüntülü reklam" oluşturun ve kimliklerini Netlify değişkeni olarak girin:
   - `ADSENSE_SLOT_TOP` (sayfa üstü), `ADSENSE_SLOT_IN_CONTENT` (içerik arası),
     `ADSENSE_SLOT_SIDEBAR` (masaüstü yan kolon)
   - Alternatif: yalnızca **Otomatik reklamlar**ı açabilirsiniz (slot girmeden).
5. **Gizlilik ve mesajlaşma** → Avrupa (GDPR) onay mesajını açın (ücretsiz, zorunlu).

### Trafik büyüdükçe daha yüksek ödeyen ağlar
| Ağ | Eşik (yaklaşık) | Not |
|---|---|---|
| Ezoic | eşik yok | AdSense'ten genelde daha yüksek |
| Journey by Mediavine | ~1.000 oturum/ay | |
| Raptive (AdThrive) | ~25.000 sayfa görüntüleme/ay | |
| Mediavine | ~50.000 oturum/ay | ABD trafiğinde en yüksek RPM'lerden |

---

## 4. Affiliate (ortaklık) geliri

Sitede ZIP ve şehir sayfalarında **"Moving to …?"** bölümü hazır. Aşağıdaki
programlardan onay aldıkça bağlantı şablonunu Netlify değişkeni olarak girmeniz yeterli;
kart otomatik olarak görünür. Şablonlarda şu yer tutucular kullanılabilir:
`{zip} {city} {state} {stateFull} {lat} {lng} {q}`

| Variable | Önerilen program (ücretsiz başvuru) | Örnek şablon |
|---|---|---|
| `AFF_HOTELS_URL` | **Stay22** (anında onay, konuma göre otel) | `https://www.stay22.com/allez/roam?aid=SIZIN_AID&lat={lat}&lng={lng}&campaign=zip-{zip}` |
| `AFF_MOVERS_URL` | Taşınma teklifleri: moveBuddha, HireAHelper, PODS (Impact / CJ ağları) | programın verdiği link + `?zip={zip}` |
| `AFF_INTERNET_URL` | İnternet sağlayıcıları: Allconnect, HighSpeedInternet.com | programın verdiği link |
| `AFF_INSURANCE_URL` | Sigorta: EverQuote, MediaAlpha, SmartFinancial | programın verdiği link |
| `AFF_SECURITY_URL` | Ev güvenliği: SimpliSafe, ADT (Impact) | programın verdiği link |
| `AMAZON_TAG` | **Amazon Associates** (ör. `zipcodedetails-20`) | yalnızca etiket |

> İpucu: En hızlı başlangıç **Stay22 + Amazon Associates**. Taşınma ve internet
> sağlayıcı programları ABD'de "ZIP code" arayan kullanıcılarla en yüksek komisyonu verir.

---

## 5. Trafik büyütme (ücretsiz)

- **Search Console → Performans**: hangi ZIP'ler gösterim alıyor, onları izleyin.
- **İç bağlantılar hazır**: her sayfa şehir/ilçe/eyalet/yakın ZIP'lere bağlanıyor.
- **Yapay zekâ aramaları (GEO/AEO)**: `llms.txt`, `llms-full.txt`, FAQ/Place/Dataset
  şemaları ve robots.txt'te yapay zekâ botlarına izin hazır.
- **Paylaşılabilir içerik**: "Richest ZIP codes", "Most expensive ZIP codes" listelerini
  Reddit (r/dataisbeautiful, eyalet alt forumları), Pinterest ve X'te paylaşın.
- **Pinterest** doğrulaması: Netlify değişkeni `PINTEREST_VERIFICATION`.
- Büyük içerik değişikliğinden sonra: GitHub → Actions → Build and Deploy → Run workflow → **Submit ALL URLs** işaretli (IndexNow).

---

## Sorun giderme

- **Deploy başarısız**: Netlify → Deploys → başarısız deploy'a tıklayın; log oradadır.
- **Değişiklik görünmüyor**: Tarayıcıda Ctrl+F5; deploy ~10 dk sürer.
- **GitHub'daki eski `NETLIFY_AUTH_TOKEN`** süresi dolmuş durumda; artık gerekli değil.
  (İsterseniz yenileyip GitHub Variable `DEPLOY_WITH_ACTIONS=true` ile Actions'tan da deploy edebilirsiniz.)
