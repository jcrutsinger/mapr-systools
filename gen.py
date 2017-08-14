from faker import Factory
from faker.providers import BaseProvider
import uuid
import random
from datetime import datetime

class YesNoProvider(BaseProvider):

    __provider__ = "yesnos"
    __lang__     = "en_US"

    yesnos = [
        u'Y', u'N'
    ]

    @classmethod
    def yesno(cls):
        return cls.random_element(cls.yesnos)

class SegmentProvider(BaseProvider):

    __provider__ = "segment"
    __lang__     = "en_US"

    segments = [
        u'25-35 Women|income-100k|children-2', u'35-45 Men|Asian|children-3',u'Male',u'Female',u'going to college',u'buying a house',u'back-to-school',u'valentines-shoppers',u'deal seekers',u'luxury shoppers',u'online-buyers',u'Fashion Insider',u'Tech Enthusiast',u'Fitness Buff',u'Gear Head',u'Caucasian',u'French',u'American',u'African American'
    ]

    @classmethod
    def segment(cls):
        return cls.random_element(cls.segments)

class BrowserProvider(BaseProvider):

    __provider__ = "browsers"
    __lang__     = "en_US"

    browsers = [
        u'safari', u'chrome', u'firefox'
    ]

    @classmethod
    def browser(cls):
        return cls.random_element(cls.browsers)

class DevicesProvider(BaseProvider):

    __provider__ = "devices"
    __lang__     = "en_US"

    devices = [
        u'ipad', u'iphone', u'macbook', u'android', u'windowssurface',u'BlackBerry'
    ]

    @classmethod
    def device(cls):
        return cls.random_element(cls.devices)

class CarrierProvider(BaseProvider):

    __provider__ = "carriers"
    __lang__     = "en_US"

    carriers = [
        u'ATT', u'Verizon', u'Sprint', u'Cricket', u'T-Mobile',u'MetroPcs'
    ]

    @classmethod
    def carrier(cls):
        return cls.random_element(cls.carriers)

class PixelsProvider(BaseProvider):

    __provider__ = "pixels"
    __lang__     = "en_US"

    pixels = [
        u'760 x 420', u'955 x 600', u'990 x 560', u'1020 x 750', u'1020 x 750',u'640 x 480',u'1024 x 768',u'1024 x 768',u'1024 x 768'
    ]

    @classmethod
    def pixel(cls):
        return cls.random_element(cls.pixels)

class AdUnitSize(BaseProvider):

    __provider__ = "adunits"
    __lang__     = "en_US"

    adunits = [
        u'8 x 2', u'2 x 8', u'2 x 4', u'2 x 6', u'4 x 4',u'8 x 2',u'8 x 3',u'8 x 1',u'1 x 2'
    ]

    @classmethod
    def adunitsize(cls):
        return cls.random_element(cls.adunits)

class TargetProvider(BaseProvider):

    __provider__ = "targets"
    __lang__     = "en_US"

    targets = [
        u'targeted', u'non-targeted'
    ]

    @classmethod
    def target(cls):
        return cls.random_element(cls.targets)

class DomainProvider(BaseProvider):

    __provider__ = "domains"
    __lang__     = "en_US"

    domains = [
        u'com', u'edu', u'gov', u'uk', u'org', u'net', u'biz', u'co', u'in', u'jp'
    ]

    @classmethod
    def domain(cls):
        return cls.random_element(cls.domains)

class MobileCapability(BaseProvider):

    __provider__ = "phonecalls"
    __lang__     = "en_US"

    phonecalls = [
        u'Phone calls', u'""'
    ]

    @classmethod
    def mobilecapable(cls):
        return cls.random_element(cls.phonecalls)

class MobileProvider(BaseProvider):

    __provider__ = "mobiles"
    __lang__     = "en_US"

    mobiles = [
        u'iphone', u'ipad', u'blackberry', u'samsung galaxy', u'motorola',u'nexus','nokia'
    ]

    @classmethod
    def mobile(cls):
        return cls.random_element(cls.mobiles)

class OsProvider(BaseProvider):

    __provider__ = "oss"
    __lang__     = "en_US"

    oss  = [
        u'Blackberry OS 7.1', u'Windows OS 2012', u'Apple IOS 10.0.1', u'BADA 2.0.5', u'Palm OS 2007',u'Android 7.0','Android 6.1',u'Apple IOS 9.1',u' Android 5.9'
    ]

    @classmethod
    def os(cls):
        return cls.random_element(cls.oss)

class PodPosition(BaseProvider):

    __provider__ = "pods"
    __lang__     = "en_US"

    pods = [
        u'Unknown', u'1st pod position',u'2nd pod position',u'3rd pod position'
    ]

    @classmethod
    def pod(cls):
        return cls.random_element(cls.pods)

class ProductProvider(BaseProvider):

    __provider__ = "products"
    __lang__     = "en_US"

    products = [
        u'Ad Server', u'AdExchange',u'AdSense',u'First Look'
    ]

    @classmethod
    def product(cls):
        return cls.random_element(cls.products)

fake = Factory.create('en_US')
fake.add_provider(DevicesProvider)
fake.add_provider(CarrierProvider)
fake.add_provider(MobileProvider)
fake.add_provider(PixelsProvider)
fake.add_provider(OsProvider)
fake.add_provider(YesNoProvider)
fake.add_provider(SegmentProvider)
fake.add_provider(BrowserProvider)
fake.add_provider(TargetProvider)
fake.add_provider(DomainProvider)
fake.add_provider(MobileCapability)
fake.add_provider(PodPosition)
fake.add_provider(ProductProvider)
fake.add_provider(AdUnitSize)



# output the column names
print "create_date"+","+"ID"+","+"ActiveViewEligibleImpression"+","+"AdUnitId"+","+"AdvertiserId"+","+"AudienceSegmentIds"+","+"BandwidthGroupId"+","+"Browser"+","+"BrowserId"+","+"City"+","+"CityId"+","+"Country"+","+"CountryId"+","+"CreativeId"+","+"CreativeSize"+","+"CreativeVersion"+","+"CustomTargeting"+","+"DeviceCategory"+","+"Domain"+","+"EstimatedBackfillRevenue"+","+"GfpContentId"+","+"IsCompanion"+","+"IsInterstitial"+","+"Keypart"+","+"LineItemId"+","+"Metro"+","+"MetroId"+","+"MobileCapability"+","+"MobileCarrier"+","+"MobileDevice"+","+"OrderId"+","+"OS"+","+"OSId"+","+"OSVersion"+","+"PodPosition"+","+"PostalCode"+","+"PostalCodeId"+","+"Product"+","+"PublisherProvidedID"+","+"Region"+","+"RegionId"+","+"RequestedAdUnitSizes"+","+"RefererURL"+","+"TargetedCustomCriteria"+","+"Time"+","+"TimeUsec"+","+"TimeUsec2"+","+"UserId"+","+"VideoFallbackPosition"+","+"VideoPosition"
for i in range(1, 1001):
    d = str(fake.date_time_between(start_date='-2d', end_date="now",tzinfo=None))
    f = datetime.strptime(d ,"%Y-%m-%d %H:%M:%S")
    print str(f.strftime('%Y-%m-%d'))+","+str(i)+","+fake.yesno()+","+str(fake.random_number())+","+str(fake.random_number())+","+fake.segment()+","+str(fake.random_number(1,7))+","+fake.browser()+","+str(fake.random_number(1,3))+","+fake.city()+","+str(fake.random_number())+",United Sates,US,"+str(fake.random_number())+","+fake.pixel()+","+str(round(random.uniform(0.1, 2.9),1))+","+fake.target()+","+fake.device()+","+fake.domain()+","+str(round(random.uniform(1.0, 99.9),2))+","+str(fake.random_number())+","+str(fake.boolean())+","+str(fake.boolean())+","+fake.md5()+","+str(fake.random_number())+","+fake.city()+","+str(fake.random_number())+","+fake.mobilecapable()+","+fake.carrier()+","+fake.mobile()+","+str(fake.random_number())+","+fake.os()+","+str(fake.random_number())+","+str(fake.random_number())+","+fake.pod()+","+fake.postalcode()+","+str(fake.random_number())+","+fake.product()+","+fake.md5()+","+fake.city()+","+str(fake.random_number())+","+fake.adunitsize()+","+fake.url()+","+fake.segment()+","+str(fake.date_time())+","+str(fake.unix_time())+","+str(fake.unix_time())+","+fake.uuid4()+","+str(fake.random_number(1,3))+","+str(fake.random_number(1,5))
