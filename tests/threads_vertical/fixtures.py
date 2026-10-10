"""Declared synthetic source pages. Never acquisition or market evidence."""

from dsp_collector.sources.crawler.threads.post_parser import _compact_post

POLICY = {"status": "SYNTHETIC", "reference": "threads-vertical-fixture-v1"}


def post(number, *, revision=0):
    texts = [
        "Pickleball tìm sân Hà Nội, hỏi giá thuê bao nhiêu? #pickleball",
        "Pickleball Đà Nẵng ghép kèo, cần thêm 2 người",
        "Pickleball Huế đặt sân tối mai",
        "Pickleball mặt sân trơn đèn tối",
        "Pickleball khai trương nhận đặt lịch",
    ]
    native = {
        "pk": str(number),
        "code": "fixture" + str(number),
        "caption": {"text": texts[number % len(texts)] + f" fixture {number}"},
        "taken_at": 1791588600,
        "edited_at": 1791588700 if revision else None,
        "language": "vi",
        "user": {"username": "synthetic_fixture", "pk": "999"},
        "like_count": max(0, number - revision),
        "view_count": number * 10,
        "text_post_app_info": {
            "direct_reply_count": 2 if number <= 3 else 0,
            "repost_count": number % 3,
            "quote_count": 0,
        },
    }
    if number % 7 == 0:
        native.pop("like_count")
    if number % 11 == 0:
        native["like_and_view_counts_disabled"] = True
    result = _compact_post(native)
    if number <= 3:
        replies = []
        for j in (1, 2):
            reply_native = {
                "pk": str(number * 10000 + j),
                "code": f"reply{number}-{j}",
                "caption": {
                    "text": "Giá thuê ở Huế bao nhiêu?" if j == 2 else "Cảm ơn bạn"
                },
                "taken_at": 1791588700,
                "user": {"username": "synthetic_reply"},
                "like_count": 0,
                "text_post_app_info": {
                    "is_reply": True,
                    "reply_to_post_id": (
                        str(number) if j == 1 else str(number * 10000 + 1)
                    ),
                },
            }
            reply = _compact_post(reply_native)
            reply["reply_to_post_id"] = reply_native["text_post_app_info"][
                "reply_to_post_id"
            ]
            replies.append(reply)
        result["detail"] = {
            **_compact_post(native),
            "visible_replies": replies,
            "reply_extraction_status": "VISIBLE_PARTIAL",
        }
    result["detail_status"] = "OK"
    return result


class FixtureFetcher:
    def __init__(self, revision=0):
        self.calls = 0
        self.revision = revision

    def __call__(self, _spec, _seen, _budget, _pages):
        self.calls += 1
        numbers = range(1, 101) if self.calls == 1 else range(51, 151)
        return {
            "posts": [post(i, revision=self.revision) for i in numbers],
            "requests": 1,
            "limitation": "SYNTHETIC_PUBLIC_PAGE",
        }
