import { revalidateTag } from "next/cache";
import { NextRequest, NextResponse } from "next/server";

import { DASHBOARD_DATA_TAG } from "@/lib/r2";

export async function POST(request: NextRequest): Promise<NextResponse> {
  const secret = request.nextUrl.searchParams.get("secret");

  if (secret !== process.env.REVALIDATE_SECRET) {
    return NextResponse.json({ revalidated: false, error: "invalid secret" }, { status: 401 });
  }

  revalidateTag(DASHBOARD_DATA_TAG, "max");
  return NextResponse.json({ revalidated: true });
}
