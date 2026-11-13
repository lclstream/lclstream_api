import { type InfiniteData, useInfiniteQuery } from "@tanstack/react-query"
import type { AxiosError } from "axios"
import { useMemo } from "react"
import type {
  GetTransfersTransfersGetError,
  GetTransfersTransfersGetResponse,
  TransferPublic,
} from "@/client"
import { getTransfersTransfersGetQueryKey } from "@/client/@tanstack/react-query.gen"
import { TransfersService } from "@/client/sdk.gen"

const PAGE_SIZE = 30

export function useTransfersQuery() {
  const baseOptions = { query: { limit: PAGE_SIZE } }

  const {
    data,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
    isPending,
    isError,
  } = useInfiniteQuery<
    GetTransfersTransfersGetResponse,
    AxiosError<GetTransfersTransfersGetError>,
    InfiniteData<GetTransfersTransfersGetResponse>,
    ReturnType<typeof getTransfersTransfersGetQueryKey>,
    number
  >({
    queryKey: getTransfersTransfersGetQueryKey(baseOptions),
    queryFn: async ({ pageParam, signal }) => {
      const { data } = await TransfersService.getTransfersTransfersGet({
        query: { ...baseOptions.query, skip: pageParam },
        signal,
        throwOnError: true,
      })
      return data
    },
    initialPageParam: 0,
    getNextPageParam: (lastPage, allPages) => {
      const loaded = allPages.reduce((sum, page) => sum + page.data.length, 0)
      return loaded < lastPage.count ? loaded : undefined
    },
    refetchInterval: 5_000,
    refetchIntervalInBackground: false,
  })

  const transfers = useMemo<TransferPublic[]>(
    () => data?.pages.flatMap((page) => page.data) ?? [],
    [data],
  )

  return {
    transfers,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
    isPending,
    isError,
  }
}
